/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CruiseControlMetricsReporterTest {
    private final static String NAME = "my-cluster";
    private final static String NAMESPACE = "my-namespace";

    private final static Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .withNewKafka()
                .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();

    @Test
    public void testDisabledCruiseControl() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewSpec()
                .endSpec()
                .build();

        assertThat(CruiseControlMetricsReporter.fromCrd(kafka, null, 3), is(nullValue()));
    }

    @Test
    public void testEnabledCruiseControlWithDefaults() {
        CruiseControlMetricsReporter ccmr = CruiseControlMetricsReporter.fromCrd(KAFKA, new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, Set.of()), 3);

        assertThat(ccmr, is(notNullValue()));
        assertThat(ccmr.numPartitions(), is(1));
        assertThat(ccmr.replicationFactor(), is(1));
        assertThat(ccmr.minInSyncReplicas(), is(1));
        assertThat(ccmr.topicName(), is(CruiseControlConfigurationParameters.DEFAULT_METRIC_REPORTER_TOPIC_NAME));
    }

    @Test
    public void testEnabledCruiseControlWithSettingsFromKafka() {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("default.replication.factor", 3);
        userConfiguration.put("min.insync.replicas", 2);
        userConfiguration.put("num.partitions", 20);

        CruiseControlMetricsReporter ccmr = CruiseControlMetricsReporter.fromCrd(KAFKA, new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet()), 3);

        assertThat(ccmr, is(notNullValue()));
        assertThat(ccmr.numPartitions(), is(20));
        assertThat(ccmr.replicationFactor(), is(3));
        assertThat(ccmr.minInSyncReplicas(), is(1)); // This does not inherit the Kafka value
        assertThat(ccmr.topicName(), is(CruiseControlConfigurationParameters.DEFAULT_METRIC_REPORTER_TOPIC_NAME));
    }

    @Test
    public void testEnabledCruiseControlWithSettingsFromCC() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewSpec()
                    .withNewCruiseControl()
                        .withConfig(Map.of("metric.reporter.topic", "my-custom-topic"))
                    .endCruiseControl()
                .endSpec()
                .build();

        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("cruise.control.metrics.topic.num.partitions", 50);
        userConfiguration.put("cruise.control.metrics.topic.replication.factor", 5);
        userConfiguration.put("cruise.control.metrics.topic.min.insync.replicas", 4);
        userConfiguration.put("default.replication.factor", 3);
        userConfiguration.put("min.insync.replicas", 2);
        userConfiguration.put("num.partitions", 20);

        CruiseControlMetricsReporter ccmr = CruiseControlMetricsReporter.fromCrd(kafka, new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet()), 5);

        assertThat(ccmr, is(notNullValue()));
        assertThat(ccmr.numPartitions(), is(nullValue()));
        assertThat(ccmr.replicationFactor(), is(nullValue()));
        assertThat(ccmr.minInSyncReplicas(), is(nullValue())); // This does not inherit the Kafka value
        assertThat(ccmr.topicName(), is("my-custom-topic"));
    }

    @Test
    public void testValidationWithOneBroker() {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("default.replication.factor", 3);
        userConfiguration.put("min.insync.replicas", 2);
        userConfiguration.put("num.partitions", 20);

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> CruiseControlMetricsReporter.fromCrd(KAFKA, new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet()), 1));

        assertThat(e.getMessage(), is("Kafka my-namespace/my-cluster has invalid configuration. Cruise Control cannot be deployed with a Kafka cluster which has only one broker. It requires at least two Kafka brokers."));
    }

    @Test
    public void testValidationWithReplicasMoreThanBrokers() {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("cruise.control.metrics.topic.replication.factor", 5);
        userConfiguration.put("cruise.control.metrics.topic.min.insync.replicas", 4);

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> CruiseControlMetricsReporter.fromCrd(KAFKA, new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet()), 2));

        assertThat(e.getMessage(), is("Kafka my-namespace/my-cluster has invalid configuration. Cruise Control metrics reporter replication factor (5) cannot be higher than number of brokers (2)."));
    }

    @Test
    public void testValidationWithReplicasMoreThanMinIsr() {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("cruise.control.metrics.topic.replication.factor", 3);
        userConfiguration.put("cruise.control.metrics.topic.min.insync.replicas", 4);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CruiseControlMetricsReporter.fromCrd(KAFKA, new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet()), 3));

        assertThat(e.getMessage(), is("The Cruise Control metric topic minISR was set to a value (4) which is higher than the number of replicas for that topic (3). Please ensure that the Cruise Control metrics topic minISR is <= to the topic's replication factor."));
    }
}
