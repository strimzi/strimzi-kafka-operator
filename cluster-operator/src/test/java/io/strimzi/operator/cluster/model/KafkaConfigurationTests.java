/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaConfigurationTests {

    KafkaVersion kafkaVersion = KafkaVersionTestUtils.getKafkaVersionLookup().defaultVersion();

    void assertConfigError(String key, Object value, String errorMsg) {
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(singletonMap(key, value).entrySet());
        assertThat(kafkaConfiguration.validate(kafkaVersion), is(singletonList(errorMsg)));
    }

    @Test
    public void unknownConfigIsNotAnError() {
        assertNoError("foo", true);
    }

    private void assertNoError(String foo, Object value) {
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(singletonMap(foo, value).entrySet());
        kafkaConfiguration.validate(kafkaVersion);
    }

    @Test
    public void outOfBoundsLong() {
        assertConfigError("log.flush.interval.messages", -2,
                "log.flush.interval.messages has value -2 which less than the minimum value 1");
    }

    @Test
    public void outOfBoundsInt() {
        assertConfigError("log.flush.offset.checkpoint.interval.ms", Long.MAX_VALUE,
                "log.flush.offset.checkpoint.interval.ms has value '9223372036854775807' which is not an int");
    }

    @Test
    public void wrongType() {
        assertConfigError("log.cleaner.io.buffer.load.factor", true,
                "log.cleaner.io.buffer.load.factor has value 'true' which is not a double");
    }

    @Test
    public void notAValidValue() {
        assertConfigError("log.message.timestamp.type", "dog",
                "log.message.timestamp.type has value 'dog' which is not one of the allowed values: [CreateTime, LogAppendTime]");
    }

    @Test
    public void listContainsInvalidItem() {
        assertConfigError("log.cleanup.policy", "csat, delete",
                "log.cleanup.policy contains values [csat] which are not in the allowed items [compact, delete]");
    }

    @Test
    public void classType() {
        assertNoError("principal.builder.class", "dof");
    }

    @Test
    public void doubleType() {
        assertNoError("sasl.kerberos.ticket.renew.jitter", 101);
    }

    @Test
    public void booleanType() {
        assertNoError("auto.create.topics.enable", "false");
    }

    @Test
    public void passwordType() {
        assertNoError("delegation.token.master.key", "dclncswn");
    }

    @Test
    public void invalidVersion() {
        assertConfigError("inter.broker.protocol.version", "dclncswn",
                "inter.broker.protocol.version has value 'dclncswn' which does not match the required pattern: \\Q0.8.0\\E(\\.[0-9]+)*|\\Q0.8.0\\E|\\Q0.8.1\\E(\\.[0-9]+)*|\\Q0.8.1\\E|\\Q0.8.2\\E(\\.[0-9]+)*|\\Q0.8.2\\E|\\Q0.9.0\\E(\\.[0-9]+)*|\\Q0.9.0\\E|\\Q0.10.0\\E(\\.[0-9]+)*|\\Q0.10.0-IV0\\E|\\Q0.10.0-IV1\\E|\\Q0.10.1\\E(\\.[0-9]+)*|\\Q0.10.1-IV0\\E|\\Q0.10.1-IV1\\E|\\Q0.10.1-IV2\\E|\\Q0.10.2\\E(\\.[0-9]+)*|\\Q0.10.2-IV0\\E|\\Q0.11.0\\E(\\.[0-9]+)*|\\Q0.11.0-IV0\\E|\\Q0.11.0-IV1\\E|\\Q0.11.0-IV2\\E|\\Q1.0\\E(\\.[0-9]+)*|\\Q1.0-IV0\\E|\\Q1.1\\E(\\.[0-9]+)*|\\Q1.1-IV0\\E|\\Q2.0\\E(\\.[0-9]+)*|\\Q2.0-IV0\\E|\\Q2.0-IV1\\E|\\Q2.1\\E(\\.[0-9]+)*|\\Q2.1-IV0\\E|\\Q2.1-IV1\\E|\\Q2.1-IV2\\E|\\Q2.2\\E(\\.[0-9]+)*|\\Q2.2-IV0\\E|\\Q2.2-IV1\\E|\\Q2.3\\E(\\.[0-9]+)*|\\Q2.3-IV0\\E|\\Q2.3-IV1\\E|\\Q2.4\\E(\\.[0-9]+)*|\\Q2.4-IV0\\E|\\Q2.4-IV1\\E|\\Q2.5\\E(\\.[0-9]+)*|\\Q2.5-IV0\\E|\\Q2.6\\E(\\.[0-9]+)*|\\Q2.6-IV0\\E|\\Q2.7\\E(\\.[0-9]+)*|\\Q2.7-IV0\\E|\\Q2.7-IV1\\E|\\Q2.7-IV2\\E");
    }

    @Test
    public void validVersion() {
        assertNoError("inter.broker.protocol.version", "2.5-IV0");
    }
}
