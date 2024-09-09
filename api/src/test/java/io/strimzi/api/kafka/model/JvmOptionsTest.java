/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.test.ReadWriteUtils;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class JvmOptionsTest {
    @Test
    public void testSetXmxXms() {
        JvmOptions opts = ReadWriteUtils.readObjectFromYamlString("-Xmx: 2g\n" +
                                                   "-Xms: 1g",
                JvmOptions.class);

        assertThat(opts.getXms(), is("1g"));
        assertThat(opts.getXmx(), is("2g"));
    }

    @Test
    public void testEmptyXmxXms() {
        JvmOptions opts = ReadWriteUtils.readObjectFromYamlString("{}", JvmOptions.class);

        assertThat(opts.getXms(), is(nullValue()));
        assertThat(opts.getXmx(), is(nullValue()));
    }

    @Test
    public void testXx() {
        JvmOptions opts = ReadWriteUtils.readObjectFromYamlString("-XX:\n" +
                                                   "  key1: value1\n" +
                                                   "  key2: value2\n" +
                                                   "  key3: true\n" +
                                                   "  key4: true\n" +
                                                   "  key5: 10\n",
                JvmOptions.class);

        assertThat(opts.getXx(), is(Map.of("key1", "value1", "key2", "value2", "key3", "true", "key4", "true", "key5", "10")));

        opts = ReadWriteUtils.readObjectFromYamlString("{}", JvmOptions.class);

        assertThat(opts.getXx(), is(nullValue()));
    }
}

