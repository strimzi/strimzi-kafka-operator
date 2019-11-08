/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.strimzi.operator.common.Util.parseMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UtilTest {
    @Test
    public void testParseMap() {
        String stringMap = "key1=value1\n" +
                "key2=value2";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m.size(), is(2));
        assertThat(m.get("key1"), is("value1"));
        assertThat(m.get("key2"), is("value2"));
    }

    @Test
    public void testParseMapNull() {
        Map<String, String> m = parseMap(null);
        assertThat(m.size(), is(0));
    }

    @Test
    public void testParseMapEmptyString() {
        String stringMap = "";

        Map<String, String> m = parseMap(null);
        assertThat(m.size(), is(0));
    }

    @Test
    public void testParseMapEmptyValue() {
        String stringMap = "key1=value1\n" +
                "key2=";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m.size(), is(2));
        assertThat(m.get("key1"), is("value1"));
        assertThat(m.get("key2"), is(""));
    }

    @Test
    public void testParseMapInvalid() {
        assertThrows(RuntimeException.class, () -> {
            String stringMap = "key1=value1\n" +
                    "key2";

            Map<String, String> m = parseMap(stringMap);
            assertThat(m.size(), is(2));
            assertThat(m.get("key1"), is("value1"));
            assertThat(m.get("key2"), is(""));
        });
    }

    @Test
    public void testParseMapValueWithEquals() {
        String stringMap = "key1=value1\n" +
                "key2=value2=value3";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m.size(), is(2));
        assertThat(m.get("key1"), is("value1"));
        assertThat(m.get("key2"), is("value2=value3"));
    }
}
