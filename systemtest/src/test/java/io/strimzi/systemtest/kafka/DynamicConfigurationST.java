/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

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

@Tag(REGRESSION)
public class DynamicConfigurationST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DynamicConfigurationST.class);
    private static final String NAMESPACE = "kafka-configuration-cluster-test";

    @Test
    void testBackgroundThreads() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.background_threads, 12);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.background_threads, 12);
    }

    @Tag(ACCEPTANCE)
    @Test
    void testCompressionType() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "snappy");
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.compression_type, "snappy");

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "gzip");
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.compression_type, "gzip");

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "lz4");
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.compression_type, "lz4");

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.compression_type, "zstd");
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.compression_type, "zstd");

    }

    @Test
    void testLogFlush() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_flush_interval_ms, 20);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_flush_interval_ms, 20);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_flush_interval_messages, 300);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_flush_interval_messages, 300);

    }

    @Test
    void testLogRetention() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_retention_ms, 20);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_retention_ms, 20);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_retention_bytes, 250);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_retention_bytes, 250);
    }

    @Test
    void testLogSegment() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_segment_bytes, 1_100);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_segment_bytes, 1_100);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_segment_delete_delay_ms, 400);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_segment_delete_delay_ms, 400);
    }

    @Test
    void testLogRoll() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_roll_jitter_ms, 500);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_roll_jitter_ms, 500);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_roll_ms, 300);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_roll_ms, 300);
    }

    @Test
    void testLogCleaner() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_backoff_ms, 10);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_backoff_ms, 10);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_dedupe_buffer_size, 4_000);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_dedupe_buffer_size, 4_000);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_delete_retention_ms, 1_000);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_delete_retention_ms, 1_000);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_io_buffer_load_factor, 12);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_io_buffer_load_factor, 12);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_io_buffer_size, 10_000);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_io_buffer_size, 10_000);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_io_max_bytes_per_second, 1.523);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_io_max_bytes_per_second, 1.523);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_max_compaction_lag_ms, 32_000);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_max_compaction_lag_ms, 32_000);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_min_cleanable_ratio, 0.3);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_min_cleanable_ratio, 0.3);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_min_compaction_lag_ms, 1);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_min_compaction_lag_ms, 1);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleaner_threads, 0);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleaner_threads, 0);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_cleanup_policy, Arrays.asList("compact", "delete"));
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_cleanup_policy, Arrays.asList("compact", "delete"));
    }

    @Test
    void testInSyncReplicasNumIoNumNetworkNumRecoveryNumReplicaFetchers() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.min_insync_replicas, 1);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.min_insync_replicas, 1);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.num_io_threads, 4);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.num_io_threads, 4);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.num_network_threads, 2);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.num_network_threads, 2);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.num_recovery_threads_per_data_dir, 3);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.num_recovery_threads_per_data_dir, 3);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.num_replica_fetchers, 1);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.num_replica_fetchers, 1);
    }

    @Test
    void testLogIndexLogMessageLogMessage() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_index_interval_bytes, 1024);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_index_interval_bytes, 1024);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_index_size_max_bytes, 5);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_index_size_max_bytes, 5);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_message_timestamp_difference_max_ms, 12_000);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_message_timestamp_difference_max_ms, 12_000);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_message_timestamp_type, "CreateTime");
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_message_timestamp_type, "CreateTime");

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_message_downconversion_enable, true);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_message_downconversion_enable, true);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.log_preallocate, true);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.log_preallocate, true);
    }

    @Test
    void testMaxConnections() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.max_connections, 10);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.max_connections, 10);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.max_connections_per_ip, 20);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.max_connections_per_ip, 20);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.max_connections_per_ip_overrides, "");
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.max_connections_per_ip_overrides, "");
    }

    @Test
    void testMetricReportersMessageMaxUncleanLeaderElection() {
        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.unclean_leader_election_enable, true);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.unclean_leader_election_enable, true);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.message_max_bytes, 2048);
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.message_max_bytes, 2048);

        assert KafkaUtils.replaceAndVerifyCrDynamicConfiguration(CLUSTER_NAME, KafkaDynamicConfiguration.metric_reporters, "");
        assert KafkaUtils.verifyKafkaPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaDynamicConfiguration.metric_reporters, "");
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        LOGGER.info("Deploying shared Kafka across all test cases!");
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();
    }
}
