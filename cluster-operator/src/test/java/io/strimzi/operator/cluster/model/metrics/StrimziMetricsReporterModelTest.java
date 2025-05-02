/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpecBuilder;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrimziMetricsReporterModelTest {
    @Test
    public void testDisabled() {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () -> new StrimziMetricsReporterModel(new KafkaConnectSpecBuilder().build(), List.of(".*")));
        assertThat(ex.getMessage(), is("Unexpected empty metrics config"));
    }

    @Test
    public void testEnabled() {
        StrimziMetricsReporter metricsConfig = new StrimziMetricsReporterBuilder()
                .withNewValues()
                    .withAllowList(List.of("kafka_log.*", "kafka_network.*"))
                .endValues()
                .build();
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(metricsConfig).build(), List.of(".*"));

        assertThat(metrics.getAllowList().isEmpty(), is(false));
        assertThat(metrics.getAllowList(), is("kafka_log.*,kafka_network.*"));
    }

    @Test
    public void testValidation() {
        assertDoesNotThrow(() -> StrimziMetricsReporterModel.validate(new StrimziMetricsReporterBuilder()
                .withNewValues()
                    .withAllowList(List.of("kafka_log.*", "kafka_network.*"))
                .endValues()
                .build())
        );

        InvalidResourceException ise0 = assertThrows(InvalidResourceException.class, () -> StrimziMetricsReporterModel.validate(
                new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withAllowList(List.of())
                        .endValues()
                        .build())
        );
        assertThat(ise0.getMessage(), is("Metrics configuration is invalid: [Allowlist should contain at least one element]"));

        InvalidResourceException ise1 = Assertions.assertThrows(InvalidResourceException.class, () -> StrimziMetricsReporterModel.validate(
                new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withAllowList(List.of("kafka_network.*", "kafka_log.***", "[a+"))
                        .endValues()
                        .build())
        );
        assertThat(ise1.getMessage(), is("Metrics configuration is invalid: [Invalid regex: kafka_log.***, Dangling meta character '*', Invalid regex: [a+, Unclosed character class]"));
    }
}
