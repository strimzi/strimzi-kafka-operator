/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.api.kafka.model.common.jmx.KafkaJmxAuthenticationPassword;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptions;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;


public class KafkaJmxOptionsTest {
    @Test
    public void testAuthentication() {
        KafkaJmxOptions opts = TestUtils.fromYamlString(
                "authentication:\n" +
                "  type: password",
                KafkaJmxOptions.class);

        assertThat(opts.getAuthentication(),  is(notNullValue()));
        assertThat(opts.getAuthentication().getType(),  is(KafkaJmxAuthenticationPassword.TYPE_PASSWORD));
    }

    @Test
    public void testNoJmxOpts() {
        KafkaJmxOptions opts = TestUtils.fromYamlString("{}", KafkaJmxOptions.class);

        assertThat(opts.getAuthentication(),  is(nullValue()));
        assertThat(opts.getAdditionalProperties(),  is(Collections.emptyMap()));
    }
}