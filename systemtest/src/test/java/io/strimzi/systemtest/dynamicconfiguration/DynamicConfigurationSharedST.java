/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.dynamicconfiguration;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static io.strimzi.systemtest.Constants.DYNAMIC_CONFIGURATION;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * DynamicConfigurationSharedST is responsible for verify that if we change dynamic Kafka configuration it will not
 * trigger rolling update
 * Shared -> for each test case we same configuration of Kafka resource
 */
@Tag(REGRESSION)
@Tag(DYNAMIC_CONFIGURATION)
public class DynamicConfigurationSharedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DynamicConfigurationSharedST.class);
    private static final String NAMESPACE = "kafka-configuration-shared-cluster-test";

    @ParameterizedTest
    @CsvSource({
        "background.threads, " + 12,
        "compression.type,  snappy",
        "compression.type,  gzip",
        "compression.type,  lz4",
        "compression.type,  zstd",
        "log.flush.interval.ms, " + 20,
        "log.retention.ms,  " + 20,
        "log.retention.bytes, " + 250,
        "log.segment.bytes,   " + 1_100,
        "log.segment.delete.delay.ms,  " + 400,
        "log.roll.jitter.ms, " + 500,
        "log.roll.ms, " + 300,
        "log.cleaner.dedupe.buffer.size, " + 4_000_000,
        "log.cleaner.delete.retention.ms, " + 1_000,
        "log.cleaner.io.buffer.load.factor, " + 12,
        "log.cleaner.io.buffer.size, " + 10_000,
        "log.cleaner.io.max.bytes.per.second, " + 1.523,
        "log.cleaner.max.compaction.lag.ms, " + 32_000,
        "log.cleaner.min.compaction.lag.ms, " + 1_000,
        "log.preallocate, " + true,
        "max.connections, " + 10,
        "max.connections.per.ip, " + 20,
        "unclean.leader.election.enable, " + true,
        "message.max.bytes, " + 2048,
    })
    void testLogDynamicKafkaConfigurationProperties(String kafkaDynamicConfigurationKey, Object kafkaDynamicConfigurationValue) {
        // exercise phase
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, kafkaDynamicConfigurationKey, kafkaDynamicConfigurationValue);

        // verify phase
        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, kafkaDynamicConfigurationKey, kafkaDynamicConfigurationValue), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), kafkaDynamicConfigurationKey, kafkaDynamicConfigurationValue), is(true));
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        LOGGER.info("Deploying shared Kafka across all test cases!");
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
    }
}
