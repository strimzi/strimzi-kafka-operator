/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import org.junit.Test;

import java.util.Map;

import static io.strimzi.operator.common.Util.parseMap;
import static org.junit.Assert.assertEquals;

public class UtilTest {
    @Test
    public void testParseMap() {
        String stringMap = "key1=value1\n" +
                "key2=value2";

        Map<String, String> m = parseMap(stringMap);
        assertEquals(2, m.size());
        assertEquals("value1", m.get("key1"));
        assertEquals("value2", m.get("key2"));
    }

    @Test
    public void testParseMapNull() {
        Map<String, String> m = parseMap(null);
        assertEquals(0, m.size());
    }

    @Test
    public void testParseMapEmptyString() {
        String stringMap = "";

        Map<String, String> m = parseMap(null);
        assertEquals(0, m.size());
    }

    @Test
    public void testParseMapEmptyValue() {
        String stringMap = "key1=value1\n" +
                "key2=";

        Map<String, String> m = parseMap(stringMap);
        assertEquals(2, m.size());
        assertEquals("value1", m.get("key1"));
        assertEquals("", m.get("key2"));
    }

    @Test(expected = RuntimeException.class)
    public void testParseMapInvalid() {
        String stringMap = "key1=value1\n" +
                "key2";

        Map<String, String> m = parseMap(stringMap);
        assertEquals(2, m.size());
        assertEquals("value1", m.get("key1"));
        assertEquals("", m.get("key2"));
    }

    @Test
    public void testParseMapValueWithEquals() {
        String stringMap = "key1=value1\n" +
                "key2=value2=value3";

        Map<String, String> m = parseMap(stringMap);
        assertEquals(2, m.size());
        assertEquals("value1", m.get("key1"));
        assertEquals("value2=value3", m.get("key2"));
    }
}
