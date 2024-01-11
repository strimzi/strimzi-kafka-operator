/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.api.Assertions;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@ParallelSuite
public class KafkaConfigurationTests {

    KafkaVersion kafkaVersion = KafkaVersionTestUtils.getKafkaVersionLookup().defaultVersion();

    void assertConfigError(String key, Object value, String errorMsg) {
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, singletonMap(key, value).entrySet());
        assertThat(kafkaConfiguration.validate(kafkaVersion), is(singletonList(errorMsg)));
    }

    @ParallelTest
    public void unknownConfigIsNotAnError() {
        assertNoError("foo", true);
    }

    private void assertNoError(String foo, Object value) {
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, singletonMap(foo, value).entrySet());
        kafkaConfiguration.validate(kafkaVersion);
    }

    @ParallelTest
    public void outOfBoundsLong() {
        assertConfigError("log.flush.interval.messages", -2,
                "log.flush.interval.messages has value -2 which less than the minimum value 1");
    }

    @ParallelTest
    public void outOfBoundsInt() {
        assertConfigError("log.flush.offset.checkpoint.interval.ms", Long.MAX_VALUE,
                "log.flush.offset.checkpoint.interval.ms has value '9223372036854775807' which is not an int");
    }

    @ParallelTest
    public void wrongType() {
        assertConfigError("log.cleaner.io.buffer.load.factor", true,
                "log.cleaner.io.buffer.load.factor has value 'true' which is not a double");
    }

    @ParallelTest
    public void notAValidValue() {
        assertConfigError("log.message.timestamp.type", "dog",
                "log.message.timestamp.type has value 'dog' which is not one of the allowed values: [CreateTime, LogAppendTime]");
    }

    @ParallelTest
    public void listContainsInvalidItem() {
        assertConfigError("log.cleanup.policy", "csat, delete",
                "log.cleanup.policy contains values [csat] which are not in the allowed items [compact, delete]");
    }

    @ParallelTest
    public void classType() {
        assertNoError("principal.builder.class", "dof");
    }

    @ParallelTest
    public void doubleType() {
        assertNoError("sasl.kerberos.ticket.renew.jitter", 101);
    }

    @ParallelTest
    public void booleanType() {
        assertNoError("auto.create.topics.enable", "false");
    }

    @ParallelTest
    public void passwordType() {
        assertNoError("delegation.token.master.key", "dclncswn");
    }

    @ParallelTest
    public void invalidVersion() {
        assertConfigError("inter.broker.protocol.version", "dclncswn",
                "inter.broker.protocol.version has value 'dclncswn' which does not match the required pattern: \\Q0.8.0\\E(\\.[0-9]+)*|\\Q0.8.0\\E|\\Q0.8.1\\E(\\.[0-9]+)*|\\Q0.8.1\\E|\\Q0.8.2\\E(\\.[0-9]+)*|\\Q0.8.2\\E|\\Q0.9.0\\E(\\.[0-9]+)*|\\Q0.9.0\\E|\\Q0.10.0\\E(\\.[0-9]+)*|\\Q0.10.0-IV0\\E|\\Q0.10.0-IV1\\E|\\Q0.10.1\\E(\\.[0-9]+)*|\\Q0.10.1-IV0\\E|\\Q0.10.1-IV1\\E|\\Q0.10.1-IV2\\E|\\Q0.10.2\\E(\\.[0-9]+)*|\\Q0.10.2-IV0\\E|\\Q0.11.0\\E(\\.[0-9]+)*|\\Q0.11.0-IV0\\E|\\Q0.11.0-IV1\\E|\\Q0.11.0-IV2\\E|\\Q1.0\\E(\\.[0-9]+)*|\\Q1.0-IV0\\E|\\Q1.1\\E(\\.[0-9]+)*|\\Q1.1-IV0\\E|\\Q2.0\\E(\\.[0-9]+)*|\\Q2.0-IV0\\E|\\Q2.0-IV1\\E|\\Q2.1\\E(\\.[0-9]+)*|\\Q2.1-IV0\\E|\\Q2.1-IV1\\E|\\Q2.1-IV2\\E|\\Q2.2\\E(\\.[0-9]+)*|\\Q2.2-IV0\\E|\\Q2.2-IV1\\E|\\Q2.3\\E(\\.[0-9]+)*|\\Q2.3-IV0\\E|\\Q2.3-IV1\\E|\\Q2.4\\E(\\.[0-9]+)*|\\Q2.4-IV0\\E|\\Q2.4-IV1\\E|\\Q2.5\\E(\\.[0-9]+)*|\\Q2.5-IV0\\E|\\Q2.6\\E(\\.[0-9]+)*|\\Q2.6-IV0\\E|\\Q2.7\\E(\\.[0-9]+)*|\\Q2.7-IV0\\E|\\Q2.7-IV1\\E|\\Q2.7-IV2\\E|\\Q2.8\\E(\\.[0-9]+)*|\\Q2.8-IV0\\E|\\Q2.8-IV1\\E|\\Q3.0\\E(\\.[0-9]+)*|\\Q3.0-IV0\\E|\\Q3.0-IV1\\E|\\Q3.1\\E(\\.[0-9]+)*|\\Q3.1-IV0\\E|\\Q3.2\\E(\\.[0-9]+)*|\\Q3.2-IV0\\E|\\Q3.3\\E(\\.[0-9]+)*|\\Q3.3-IV0\\E|\\Q3.3-IV1\\E|\\Q3.3-IV2\\E|\\Q3.3-IV3\\E|\\Q3.4\\E(\\.[0-9]+)*|\\Q3.4-IV0\\E|\\Q3.5\\E(\\.[0-9]+)*|\\Q3.5-IV0\\E|\\Q3.5-IV1\\E|\\Q3.5-IV2\\E|\\Q3.6\\E(\\.[0-9]+)*|\\Q3.6-IV0\\E|\\Q3.6-IV1\\E|\\Q3.6-IV2\\E|\\Q3.7\\E(\\.[0-9]+)*|\\Q3.7-IV0\\E|\\Q3.7-IV1\\E|\\Q3.7-IV2\\E|\\Q3.7-IV3\\E|\\Q3.7-IV4\\E|\\Q3.8\\E(\\.[0-9]+)*|\\Q3.8-IV0\\E");
    }

    @ParallelTest
    public void validVersion() {
        assertNoError("inter.broker.protocol.version", "2.5-IV0");
    }

    @ParallelTest
    public void unsupportedVersion() {
        RuntimeException exc = Assertions.assertThrows(RuntimeException.class, () ->
            KafkaConfiguration.readConfigModel(KafkaVersionTestUtils.getKafkaVersionLookup().version("2.6.0"))
        );

        assertThat(exc.getMessage(), containsString("Configuration model /kafka-2.6.0-config-model.json was not found"));
    }
}
