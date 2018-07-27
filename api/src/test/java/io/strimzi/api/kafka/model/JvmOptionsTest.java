/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JvmOptionsTest {
    @Test
    public void testXmxXms() {
        JvmOptions opts = TestUtils.fromJson("{" +
                "  \"-Xmx\": \"2g\"," +
                "  \"-Xms\": \"1g\"" +
                "}", JvmOptions.class);

        assertEquals("1g", opts.getXms());
        assertEquals("2g", opts.getXmx());
    }

    @Test
    public void testEmptyXmxXms() {
        JvmOptions opts = TestUtils.fromJson("{}", JvmOptions.class);

        assertNull(opts.getXms());
        assertNull(opts.getXmx());
    }

    @Test
    public void testServer() {
        JvmOptions opts = TestUtils.fromJson("{" +
                "  \"-server\": \"true\"" +
                "}", JvmOptions.class);

        assertTrue(opts.isServer());


        opts = TestUtils.fromJson("{" +
                "  \"-server\": true" +
                "}", JvmOptions.class);

        assertTrue(opts.isServer());

        opts = TestUtils.fromJson("{" +
                "  \"-server\": \"false\"" +
                "}", JvmOptions.class);

        assertFalse(opts.isServer());

        opts = TestUtils.fromJson("{}", JvmOptions.class);

        assertNull(opts.isServer());
    }

    @Test
    public void testXx() {
        JvmOptions opts = TestUtils.fromJson("{" +
                "    \"-XX\":" +
                "            {\"key1\": \"value1\"," +
                "            \"key2\": \"value2\"," +
                "            \"key3\": \"true\"," +
                "            \"key4\": true," +
                "            \"key5\": 10}" +
                "}", JvmOptions.class);

        assertEquals(TestUtils.map("key1", "value1", "key2", "value2", "key3", "true", "key4", "true", "key5", "10"), opts.getXx());
    }
}

