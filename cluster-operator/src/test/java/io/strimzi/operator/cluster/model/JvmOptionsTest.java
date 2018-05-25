/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.ResourceUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JvmOptionsTest {
    @Test
    public void testXmxXms() {
        JvmOptions opts = JvmOptions.fromJson("{" +
                "  \"-Xmx\": \"2g\"," +
                "  \"-Xms\": \"1g\"" +
                "}");

        assertEquals("1g", opts.getXms());
        assertEquals("2g", opts.getXmx());
    }

    @Test
    public void testEmptyXmxXms() {
        JvmOptions opts = JvmOptions.fromJson("{}");

        assertNull(opts.getXms());
        assertNull(opts.getXmx());
    }

    @Test
    public void testServer() {
        JvmOptions opts = JvmOptions.fromJson("{" +
                "  \"-server\": \"true\"" +
                "}");

        assertTrue(opts.getServer());


        opts = JvmOptions.fromJson("{" +
                "  \"-server\": true" +
                "}");

        assertTrue(opts.getServer());

        opts = JvmOptions.fromJson("{" +
                "  \"-server\": \"false\"" +
                "}");

        assertFalse(opts.getServer());

        opts = JvmOptions.fromJson("{}");

        assertFalse(opts.getServer());
    }

    @Test
    public void testXx() {
        JvmOptions opts = JvmOptions.fromJson("{" +
                "    \"-XX\":" +
                "            {\"key1\": \"value1\"," +
                "            \"key2\": \"value2\"," +
                "            \"key3\": \"true\"," +
                "            \"key4\": true," +
                "            \"key5\": 10}" +
                "}");

        assertEquals(ResourceUtils.map("key1", "value1", "key2", "value2", "key3", "true", "key4", "true", "key5", "10"), opts.getXx());
    }
}

