/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.oneOf;

public class KafkaConfigurationTests {

    KafkaVersion kafkaVersion = KafkaVersionTestUtils.getKafkaVersionLookup().defaultVersion();

    void assertConfigError(String key, Object value, String errorMsg) {
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, singletonMap(key, value).entrySet());
        assertThat(kafkaConfiguration.validate(kafkaVersion), is(singletonList(errorMsg)));
    }

    @Test
    public void unknownConfigIsNotAnError() {
        assertNoError("foo", true);
    }

    private void assertNoError(String foo, Object value) {
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, singletonMap(foo, value).entrySet());
        assertThat(kafkaConfiguration.validate(kafkaVersion), is(oneOf(emptyList(), nullValue())));
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
    public void unsupportedVersion() {
        RuntimeException exc = Assertions.assertThrows(RuntimeException.class, () ->
            KafkaConfiguration.readConfigModel(KafkaVersionTestUtils.getKafkaVersionLookup().version("2.6.0"))
        );

        assertThat(exc.getMessage(), containsString("Configuration model /kafka-2.6.0-config-model.json was not found"));
    }

    @Test
    public void testGzipCompressionLevel() {
        assertNoError("compression.gzip.level", "9");
        assertNoError("compression.gzip.level", "-1");
        assertNoError("compression.gzip.level", "1");
        assertConfigError("compression.gzip.level", "0", "compression.gzip.level has value '0' which does not match the required pattern: [1-9]{1}|-1");
        assertConfigError("compression.gzip.level", "10", "compression.gzip.level has value '10' which does not match the required pattern: [1-9]{1}|-1");
    }

    @Test
    public void testCaseSensitiveOptions() {
        assertNoError("compression.type", "gzip");
        assertConfigError("compression.type", "GZIP", "compression.type has value 'GZIP' which is not one of the allowed values: [uncompressed, zstd, lz4, snappy, gzip, producer]");
    }

    @Test
    public void testCaseInsensitiveOptions() {
        assertNoError("group.consumer.migration.policy", "DISABLED");
        assertNoError("group.consumer.migration.policy", "downgrade");
        assertNoError("group.consumer.migration.policy", "Upgrade");
        assertConfigError("group.consumer.migration.policy", "wrong_option", "group.consumer.migration.policy has value 'wrong_option' which is not one of the allowed values (case-insensitive): [DISABLED, DOWNGRADE, UPGRADE, BIDIRECTIONAL]");
    }

    @Test
    public void testRemoteStorageCopierThreadPoolSize() {
        assertNoError("remote.log.manager.copier.thread.pool.size", "9");
        assertNoError("remote.log.manager.copier.thread.pool.size", "1");
        assertNoError("remote.log.manager.copier.thread.pool.size", "10");
        assertNoError("remote.log.manager.copier.thread.pool.size", "16");
        assertConfigError("remote.log.manager.copier.thread.pool.size", "-1", "remote.log.manager.copier.thread.pool.size has value -1 which less than the minimum value 1");
        assertConfigError("remote.log.manager.copier.thread.pool.size", "0", "remote.log.manager.copier.thread.pool.size has value 0 which less than the minimum value 1");
        assertConfigError("remote.log.manager.copier.thread.pool.size", "-5", "remote.log.manager.copier.thread.pool.size has value -5 which less than the minimum value 1");
    }

    @Test
    public void testRemoteStorageExpirationThreadPoolSize() {
        assertNoError("remote.log.manager.expiration.thread.pool.size", "9");
        assertNoError("remote.log.manager.expiration.thread.pool.size", "1");
        assertNoError("remote.log.manager.expiration.thread.pool.size", "10");
        assertNoError("remote.log.manager.expiration.thread.pool.size", "16");
        assertConfigError("remote.log.manager.expiration.thread.pool.size", "-1", "remote.log.manager.expiration.thread.pool.size has value -1 which less than the minimum value 1");
        assertConfigError("remote.log.manager.expiration.thread.pool.size", "0", "remote.log.manager.expiration.thread.pool.size has value 0 which less than the minimum value 1");
        assertConfigError("remote.log.manager.expiration.thread.pool.size", "-5", "remote.log.manager.expiration.thread.pool.size has value -5 which less than the minimum value 1");
    }
}
