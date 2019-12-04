/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;


public class KafkaJmxOptionsTest {
    @Test
    public void testAuthentication() {
        KafkaJmxOptions opts = TestUtils.fromJson("{" +
                "\"authentication\": {" +
                    "\"password\": true" +
                    "}" +
                "}", KafkaJmxOptions.class);

        assertThat(opts.getAuthentication(),  is(notNullValue()));
        assertThat(opts.getAuthentication().getPasswordProtected(),  is(true));
    }

}
