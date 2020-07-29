package io.strimzi.systemtest.kafka;

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

import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(REGRESSION)
public class DynamicConfigurationST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DynamicConfigurationST.class);
    public static final String NAMESPACE = "kafka-configuration-cluster-test";

    @Test
    void testBackgroundThreads() {
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.background_threads, 12);
    }

    @Test
    void testCompressionType() {
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.compression_type, "snappy");
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.compression_type, "gzip");
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.compression_type, "lz4");
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.compression_type, "zstd");
    }

    @Test
    void testLogFlush() {
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.log_flush_interval_ms, 20L);
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.log_flush_interval_messages, 300L);
    }

    @Test
    void testLogRetentionRollSegment() {
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.log_retention_ms, 20L);
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.log_retention_bytes, 250L);
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.log_roll_jitter_ms, 500L);
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.log_roll_ms, 300L);
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.log_segment_bytes, 1100L);
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.log_segment_delete_delay_ms, 400L);
    }

    @Test
    void testInSyncReplicasNumIoNumNetworkNumRecoveryNumReplicaFetchers() {
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.min_insync_replicas, 1);
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.num_io_threads, 4);
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.num_network_threads, 2);
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.num_recovery_threads_per_data_dir, 3);
        KafkaUtils.verifyDynamicConfiguration(KafkaDynamicConfiguration.num_replica_fetchers, 1);
    }
    
    // TODO: more tests... (maybe create map with conf ??)

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        LOGGER.info("Deploying shared Kafka across all test cases!");
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();
    }
}
