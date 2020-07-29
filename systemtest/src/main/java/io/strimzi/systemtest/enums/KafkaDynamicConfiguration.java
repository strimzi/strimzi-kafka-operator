/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.enums;

/**
 * KafkaConfiguration enum class, which provides all supported configuration, which does not need to trigger rolling-update (dynamic configuration)
 */
public enum KafkaDynamicConfiguration {

    background_threads,
    compression_type,
    min_insync_replicas,
    unclean_leader_election_enable,
    message_max_bytes,
    metric_reporters,

    log_flush_interval_messages,
    log_flush_interval_ms,
    log_retention_bytes,
    log_retention_ms,
    log_roll_jitter_ms,
    log_roll_ms,
    log_segment_bytes,
    log_segment_delete_delay_ms,
    log_cleaner_backoff_ms,
    log_cleaner_dedupe_buffer_size,
    log_cleaner_delete_retention_ms,
    log_cleaner_io_buffer_load_factor,
    log_cleaner_io_buffer_size,
    log_cleaner_io_max_bytes_per_second,
    log_cleaner_max_compaction_lag_ms,
    log_cleaner_min_cleanable_ratio,
    log_cleaner_min_compaction_lag_ms,
    log_cleaner_threads,
    log_cleanup_policy,
    log_index_interval_bytes,
    log_index_size_max_bytes,
    log_message_timestamp_difference_max_ms,
    log_message_timestamp_type,
    log_message_downconversion_enable,
    log_preallocate,

    num_io_threads,
    num_network_threads,
    num_recovery_threads_per_data_dir,
    num_replica_fetchers,

    max_connections,
    max_connections_per_ip,
    max_connections_per_ip_overrides;

    @Override
    public String toString() {
        return this.name().replaceAll("_", ".");
    }
}
