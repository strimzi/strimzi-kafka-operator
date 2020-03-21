/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
public class JvmOptionsTest {
    @Test
    public void testSetXmxXms() {
        JvmOptions opts = TestUtils.fromJson("{" +
                "  \"-Xmx\": \"2g\"," +
                "  \"-Xms\": \"1g\"" +
                "}", JvmOptions.class);

        assertThat(opts.getXms(), is("1g"));
        assertThat(opts.getXmx(), is("2g"));
    }

    @Test
    public void testEmptyXmxXms() {
        JvmOptions opts = TestUtils.fromJson("{}", JvmOptions.class);

        assertThat(opts.getXms(), is(nullValue()));
        assertThat(opts.getXmx(), is(nullValue()));
    }

    @Test
    public void testServer() {
        JvmOptions opts = TestUtils.fromJson("{" +
                "  \"-server\": \"true\"" +
                "}", JvmOptions.class);

        assertThat(opts.isServer(), is(true));

        opts = TestUtils.fromJson("{" +
                "  \"-server\": true" +
                "}", JvmOptions.class);

        assertThat(opts.isServer(), is(true));

        opts = TestUtils.fromJson("{" +
                "  \"-server\": \"false\"" +
                "}", JvmOptions.class);

        assertThat(opts.isServer(), is(false));

        opts = TestUtils.fromJson("{" +
                "  \"-server\": false" +
                "}", JvmOptions.class);

        assertThat(opts.isServer(), is(false));

        opts = TestUtils.fromJson("{}", JvmOptions.class);

        assertThat(opts.isServer(), is(nullValue()));
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

        assertThat(opts.getXx(), is(TestUtils.map("key1", "value1", "key2", "value2", "key3", "true", "key4", "true", "key5", "10")));

        opts = TestUtils.fromJson("{}", JvmOptions.class);

        assertThat(opts.getXx(), is(nullValue()));
    }
}

