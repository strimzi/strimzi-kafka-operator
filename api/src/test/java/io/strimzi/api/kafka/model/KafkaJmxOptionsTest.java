/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaJmxOptionsTest {
    @Test
    public void testEnabled() {
        KafkaJmxOptions opts = TestUtils.fromJson("{" +
                "\"enabled\": \"true\"" +
                "}", KafkaJmxOptions.class);

        assertThat(opts.isEnabled(), is(true));
    }

    @Test
    public void testEmptyEnabled() {
        KafkaJmxOptions opts = TestUtils.fromJson("{}", KafkaJmxOptions.class);

        assertThat(opts.isEnabled(), is(false));
    }



}
