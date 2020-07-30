/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.dynamicconfiguration;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.enums.KafkaDynamicConfiguration;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

@Tag(REGRESSION)
public class DynamicConfigurationSharedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DynamicConfigurationSharedST.class);
    private static final String NAMESPACE = "kafka-configuration-shared-cluster-test";

    @Test
    void testBackgroundThreads() {
        // exercise phase
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.background_threads, 12);

        // verify phase
        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.background_threads, 12), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.background_threads, 12), is(true));
    }

    @Tag(ACCEPTANCE)
    @Test
    void testCompressionType() {
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "snappy");

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "snappy"), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.compression_type, "snappy"), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "gzip");

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "gzip"), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.compression_type, "gzip"), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "lz4");

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "lz4"), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.compression_type, "lz4"), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "zstd");

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "zstd"), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.compression_type, "zstd"), is(true));

    }

    @Test
    void testLogFlush() {
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_flush_interval_ms, 20);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_flush_interval_ms, 20), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_flush_interval_ms, 20), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_flush_interval_messages, 300);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_flush_interval_messages, 300), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_flush_interval_messages, 300), is(true));

    }

    @Test
    void testLogRetention() {
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_retention_ms, 20);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_retention_ms, 20), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_retention_ms, 20), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_retention_bytes, 250);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_retention_bytes, 250), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_retention_bytes, 250), is(true));
    }

    @Test
    void testLogSegment() {
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_segment_bytes, 1_100);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_segment_bytes, 1_100), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_segment_bytes, 1_100), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_segment_delete_delay_ms, 400);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_segment_delete_delay_ms, 400), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_segment_delete_delay_ms, 400), is(true));
    }

    @Test
    void testLogRoll() {
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_roll_jitter_ms, 500);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_roll_jitter_ms, 500), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_roll_jitter_ms, 500), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_roll_ms, 300);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_roll_ms, 300), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_roll_ms, 300), is(true));
    }

    @Test
    void testLogCleaner() {
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_backoff_ms, 10);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_backoff_ms, 10), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_backoff_ms, 10), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_dedupe_buffer_size, 4_000_000);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_dedupe_buffer_size, 4_000_000), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_dedupe_buffer_size, 4_000_000), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_delete_retention_ms, 1_000);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_delete_retention_ms, 1_000), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_delete_retention_ms, 1_000), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_io_buffer_load_factor, 12);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_io_buffer_load_factor, 12), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_io_buffer_load_factor, 12), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_io_buffer_size, 10_000);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_io_buffer_size, 10_000), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_io_buffer_size, 10_000), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_io_max_bytes_per_second, 1.523);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_io_max_bytes_per_second, 1.523), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_io_max_bytes_per_second, 1.523), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_max_compaction_lag_ms, 32_000);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_max_compaction_lag_ms, 32_000), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_max_compaction_lag_ms, 32_000), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_roll_ms, 0.3);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_min_cleanable_ratio, 0.3), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_min_cleanable_ratio, 0.3), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_min_compaction_lag_ms, 1);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_min_compaction_lag_ms, 1), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_min_compaction_lag_ms, 1), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_threads, 0);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_threads, 0), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_threads, 0), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleanup_policy, Arrays.asList("compact", "delete"));

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleanup_policy, Arrays.asList("compact", "delete")), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleanup_policy, Arrays.asList("compact", "delete")), is(true));
    }

    @Test
    void testInSyncReplicasNumIoNumNetworkNumRecoveryNumReplicaFetchers() {
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.min_insync_replicas, 1);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.min_insync_replicas, 1), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.min_insync_replicas, 1), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.num_io_threads, 4);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.num_io_threads, 4), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.num_io_threads, 4), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.num_network_threads, 2);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.num_network_threads, 2), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.num_network_threads, 2), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleanup_policy, Arrays.asList("compact", "delete"));

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.num_recovery_threads_per_data_dir, 3), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.num_recovery_threads_per_data_dir, 3), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleanup_policy, 1);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.num_replica_fetchers, 1), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.num_replica_fetchers, 1), is(true));
    }

    @Test
    void testLogIndexLogMessageLogMessage() {
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_index_interval_bytes, 1024);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_index_interval_bytes, 1024), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_index_interval_bytes, 1024), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_index_size_max_bytes, 5);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_index_size_max_bytes, 5), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_index_size_max_bytes, 5), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_message_timestamp_difference_max_ms, 12_000);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_message_timestamp_difference_max_ms, 12_000), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_message_timestamp_difference_max_ms, 12_000), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_message_timestamp_type, "CreateTime");

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_message_timestamp_type, "CreateTime"), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_message_timestamp_type, "CreateTime"), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_message_downconversion_enable, true);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_message_downconversion_enable, true), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_message_downconversion_enable, true), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.log_preallocate, true);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_preallocate, true), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_preallocate, true), is(true));
    }

    @Test
    void testMaxConnections() {
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.max_connections, 10);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.max_connections, 10), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.max_connections, 10), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.max_connections_per_ip, 20);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.max_connections_per_ip, 20), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.max_connections_per_ip, 20), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.max_connections_per_ip_overrides, "");

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.max_connections_per_ip_overrides, ""), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.max_connections_per_ip_overrides, ""), is(true));
    }

    @Test
    void testMetricReportersMessageMaxUncleanLeaderElection() {
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.unclean_leader_election_enable, true);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.unclean_leader_election_enable, true), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.unclean_leader_election_enable, true), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.message_max_bytes, 2048);

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.message_max_bytes, 2048), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.message_max_bytes, 2048), is(true));

        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, KafkaDynamicConfiguration.metric_reporters, "");

        assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.metric_reporters, ""), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.metric_reporters, ""), is(true));
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        LOGGER.info("Deploying shared Kafka across all test cases!");
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();
    }
}
