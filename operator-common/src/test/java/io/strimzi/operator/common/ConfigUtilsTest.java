/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConfigUtilsTest {
    @Test
    public void testParseValidLabels()   {
        String validLabels = "key1=value1,key2=value2";

        Map expected = new HashMap<String, String>(2);
        expected.put("key1", "value1");
        expected.put("key2", "value2");

        assertEquals(expected, ConfigUtils.parseLabels(validLabels));
    }

    @Test
    public void testParseNullLabels()   {
        String validLabels = null;
        assertEquals(Collections.EMPTY_MAP, ConfigUtils.parseLabels(validLabels));
    }

    @Test
    public void testParseEmptyLabels()   {
        String validLabels = "";
        assertEquals(Collections.EMPTY_MAP, ConfigUtils.parseLabels(validLabels));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testParseInvalidLabels1()   {
        String invalidLabels = ",key1=value1,key2=value2";

        ConfigUtils.parseLabels(invalidLabels);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testParseInvalidLabels2()   {
        String invalidLabels = "key1=value1,key2=";

        ConfigUtils.parseLabels(invalidLabels);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testParseInvalidLabels3()   {
        String invalidLabels = "key2";

        ConfigUtils.parseLabels(invalidLabels);
    }
}
