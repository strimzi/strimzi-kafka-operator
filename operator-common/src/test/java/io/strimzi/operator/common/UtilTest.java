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
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UtilTest {
    @Test
    public void testParseMap() {
        String stringMap = "key1=value1\n" +
                "key2=value2";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m, aMapWithSize(2));
        assertThat(m, hasEntry("key1", "value1"));
        assertThat(m, hasEntry("key2", "value2"));
    }

    @Test
    public void testParseMapNull() {
        Map<String, String> m = parseMap(null);
        assertThat(m, aMapWithSize(0));
    }

    @Test
    public void testParseMapEmptyString() {
        Map<String, String> m = parseMap(null);
        assertThat(m, aMapWithSize(0));
    }

    @Test
    public void testParseMapEmptyValue() {
        String stringMap = "key1=value1\n" +
                "key2=";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m, aMapWithSize(2));
        assertThat(m, hasEntry("key1", "value1"));
        assertThat(m, hasEntry("key2", ""));
    }

    @Test
    public void testParseMapInvalid() {
        assertThrows(RuntimeException.class, () -> {
            String stringMap = "key1=value1\n" +
                    "key2";

            Map<String, String> m = parseMap(stringMap);
            assertThat(m, aMapWithSize(2));
            assertThat(m, hasEntry("key1", "value1"));
            assertThat(m, hasEntry("key2", ""));
        });
    }

    @Test
    public void testParseMapValueWithEquals() {
        String stringMap = "key1=value1\n" +
                "key2=value2=value3";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m, aMapWithSize(2));
        assertThat(m, hasEntry("key1", "value1"));
        assertThat(m, hasEntry("key2", "value2=value3"));
    }

    @Test
    public void testMaskedPasswords()   {
        String noPassword = "SOME_VARIABLE";
        String passwordAtTheEnd = "SOME_PASSWORD";
        String passwordInTheMiddle = "SOME_PASSWORD_TO_THE_BIG_SECRET";

        assertThat(Util.maskPassword(noPassword, "123456"), is("123456"));
        assertThat(Util.maskPassword(passwordAtTheEnd, "123456"), is("********"));
        assertThat(Util.maskPassword(passwordInTheMiddle, "123456"), is("********"));
    }
}
