/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class KafkaJmxRemoteTest {
    @Test
    public void testSecure() {
        KafkaJmxRemote opts = TestUtils.fromJson("{" +
                "\"secure\": \"true\"" +
                "}", KafkaJmxRemote.class);

        assertThat(opts.getSecure(),  is(true));
    }

}
