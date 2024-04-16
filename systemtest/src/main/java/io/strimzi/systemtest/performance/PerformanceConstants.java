/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

/**
 * Interface for keep global constants used across performance tests.
 */
public interface PerformanceConstants {

    /**
     * IN constants represent the input parameters provided to the topic operator during performance tests.
     */

    /**
     * The number of Kafka topics to be created or managed during the test.
     */
    String TOPIC_OPERATOR_IN_NUMBER_OF_TOPICS = "IN: NUMBER OF TOPICS";

    /**
     * The number of client instances interacting with Kafka during the test.
     */
    String TOPIC_OPERATOR_IN_NUMBER_OF_CLIENT_INSTANCES = "IN: NUMBER OF CLIENTS INSTANCES";

    /**
     * The total number of messages that should be sent or received during the test.
     */
    String TOPIC_OPERATOR_IN_NUMBER_OF_MESSAGES = "IN: NUMBER OF MESSAGES";

    /**
     * The maximum size of a batch of messages in bytes. This affects how Kafka batches messages before sending them.
     */
    String TOPIC_OPERATOR_IN_MAX_BATCH_SIZE = "IN: MAX BATCH SIZE (ms)";

    /**
     * The maximum time, in milliseconds, to wait before sending a batch, even if the batch size has not been reached.
     */
    String TOPIC_OPERATOR_IN_MAX_BATCH_LINGER_MS = "IN: MAX BATCH LINGER (ms)";

    /**
     * The number of topics that will be updated during the test. This can involve changes to configurations or partitions.
     */
    String TOPIC_OPERATOR_IN_NUMBER_OF_TOPICS_TO_UPDATE = "IN: UPDATE NUMBER OF TOPICS";

    /**
     * OUT constants represent the output metrics or results measured after the performance tests involving the topic operator.
     */

    /**
     * The time taken, in milliseconds, to create all necessary Kafka topics as specified in the test parameters.
     */
    String TOPIC_OPERATOR_OUT_CREATION_TIME = "OUT: Creation Time (ms)";

    /**
     * The total time, in milliseconds, for sending and receiving all messages as part of the test.
     */
    String TOPIC_OPERATOR_OUT_SEND_AND_RECV_TIME = "OUT: Send And Recv Time (ms)";

    /**
     * The time taken, in milliseconds, to delete all Kafka topics that were created during the test.
     */
    String TOPIC_OPERATOR_OUT_DELETION_TIME = "OUT: Deletion Time (ms)";

    /**
     * The total time, in milliseconds, from the start to the end of the test, covering all operations.
     */
    String TOPIC_OPERATOR_OUT_TOTAL_TEST_TIME = "OUT: Total Test Time (ms)";

    /**
     * The time taken, in milliseconds, to update the configurations or parameters of existing Kafka topics during the test.
     */
    String TOPIC_OPERATOR_OUT_UPDATE_TIMES = "OUT: Bob Update Times (ms)";

    String METRICS_HISTORY = "Metrics History";

    String KAFKA_IN_CONFIGURATION = "IN: Kafka Configuration";

    /**
     * Metrics names
     */
    String ALTER_CONFIGS_DURATION_SECONDS_SUM = "strimzi_alter_configs_duration_seconds_sum";
    String REMOVE_FINALIZER_DURATION_SECONDS_SUM = "strimzi_remove_finalizer_duration_seconds_sum";
    String RECONCILIATIONS_DURATION_SECONDS_SUM = "strimzi_reconciliations_duration_seconds_sum";
    String CREATE_TOPICS_DURATION_SECONDS_SUM = "strimzi_create_topics_duration_seconds_sum";
    String DESCRIBE_TOPICS_DURATION_SECONDS_SUM = "strimzi_describe_topics_duration_seconds_sum";
    String CREATE_PARTITIONS_DURATION_SECONDS_SUM = "strimzi_create_partitions_duration_seconds_sum";
    String ADD_FINALIZER_DURATION_SECONDS_SUM = "strimzi_add_finalizer_duration_seconds_sum";
    String UPDATE_STATUS_DURATION_SECONDS_SUM = "strimzi_update_status_duration_seconds_sum";
    String DESCRIBE_CONFIGS_DURATION_SECONDS_SUM = "strimzi_describe_configs_duration_seconds_sum";
    String DELETE_TOPICS_DURATION_SECONDS_SUM = "strimzi_delete_topics_duration_seconds_sum";

    String RECONCILIATIONS_DURATION_SECONDS_MAX = "strimzi_reconciliations_duration_seconds_max";
    String RECONCILIATIONS_MAX_QUEUE_SIZE = "strimzi_reconciliations_max_queue_size";
    String RECONCILIATIONS_MAX_BATCH_SIZE = "strimzi_reconciliations_max_batch_size";
    String RECONCILIATIONS_SUCCESSFUL_TOTAL = "strimzi_reconciliations_successful_total";
    String RECONCILIATIONS_TOTAL = "strimzi_reconciliations_total";
    String RECONCILIATIONS_FAILED_TOTAL = "strimzi_reconciliations_failed_total";
    String RECONCILIATIONS_LOCKED_TOTAL = "strimzi_reconciliations_locked_total";

    String CREATE_TOPICS_DURATION_SECONDS_MAX = "strimzi_create_topics_duration_seconds_max";
    String DELETE_TOPICS_DURATION_SECONDS_MAX = "strimzi_delete_topics_duration_seconds_max";
    String UPDATE_STATUS_DURATION_SECONDS_MAX = "strimzi_update_status_duration_seconds_max";
    String DESCRIBE_TOPICS_DURATION_SECONDS_MAX = "strimzi_describe_topics_duration_seconds_max";
    String ALTER_CONFIGS_DURATION_SECONDS_MAX = "strimzi_alter_configs_duration_seconds_max";
    String DESCRIBE_CONFIGS_DURATION_SECONDS_MAX = "strimzi_describe_configs_duration_seconds_max";
    String ADD_FINALIZER_DURATION_SECONDS_MAX = "strimzi_add_finalizer_duration_seconds_max";
    String REMOVE_FINALIZER_DURATION_SECONDS_MAX = "strimzi_remove_finalizer_duration_seconds_max";

    String RESOURCES = "strimzi_resources";
    String JVM_GC_MEMORY_ALLOCATED_BYTES_TOTAL = "jvm_gc_memory_allocated_bytes_total";
    String JVM_THREADS_LIVE_THREADS = "jvm_threads_live_threads";
    String SYSTEM_CPU_USAGE = "system_cpu_usage";
    String SYSTEM_CPU_COUNT = "system_cpu_count";
    String JVM_MEMORY_MAX_BYTES = "jvm_memory_max_bytes";
    String PROCESS_CPU_USAGE = "process_cpu_usage";
    String SYSTEM_LOAD_AVERAGE_1M = "system_load_average_1m";

    /**
     * Derived metrics
     */
    String TOTAL_TIME_SPEND_ON_UTO_EVENT_QUEUE_DURATION_SECONDS = "strimzi_total_time_spend_on_uto_event_queue_duration_seconds";
    String SYSTEM_LOAD_AVERAGE_PER_CORE_PERCENT = "system_load_average_per_core";
    String JVM_MEMORY_USED_MEGABYTES_TOTAL = "jvm_memory_used_megabytes_total";

    /**
     * Performance use cases
     */
    String TOPIC_OPERATOR_BOBS_STREAMING_USE_CASE = "bobStreamingUseCase";
    String TOPIC_OPERATOR_ALICE_BULK_USE_CASE = "aliceBulkUseCase";

    /**
     * Performance metrics file
     */
    String PERFORMANCE_METRICS_FILE_NAME = "test-performance-metrics";

    /**
     * Polling interval of metrics in seconds
     */
    long DEFAULT_METRICS_POLLING_INTERVAL_SEC = 5;

    /**
     * Parser types
     */
    String TOPIC_OPERATOR_PARSER = "topic-operator";
}
