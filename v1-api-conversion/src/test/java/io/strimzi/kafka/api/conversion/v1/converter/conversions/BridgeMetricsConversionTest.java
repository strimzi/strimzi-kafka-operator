/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.kafka.api.conversion.v1.converter.AbstractConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaBridgeConverter;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("deprecation")
class BridgeMetricsConversionTest {
    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    protected KafkaBridge jsonConversion(AbstractConverter<KafkaBridge> converter, KafkaBridge cr, ApiVersion toApiVersion) throws Exception {
        // first do json node conversion, so we don't modify cr already
        byte[] bytes = JSON_MAPPER.writeValueAsBytes(cr);
        JsonNode node = JSON_MAPPER.readTree(bytes);
        converter.convertTo(node, toApiVersion);
        return JSON_MAPPER.readerFor(converter.crClass()).readValue(node);
    }

    protected KafkaBridge crConversion(AbstractConverter<KafkaBridge> converter, KafkaBridge cr, ApiVersion toApiVersion) {
        converter.convertTo(cr, toApiVersion);
        return cr;
    }

    @Test
    public void testBridgeMetricsConversion() throws Exception {
        KafkaBridge kb = new KafkaBridgeBuilder()
                .withApiVersion("kafka.strimzi.io/v1beta2")
                .withNewMetadata()
                    .withName("test")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withBootstrapServers("my-kafka:9092")
                    .withNewHttp()
                        .withPort(8080)
                    .endHttp()
                    .withEnableMetrics(true)
                .endSpec()
                .build();

        verify(crConversion(new KafkaBridgeConverter(), kb, ApiVersion.V1));
        verify(jsonConversion(new KafkaBridgeConverter(), kb, ApiVersion.V1));
    }

    private void verify(KafkaBridge bridge) {
        assertThat(bridge.getSpec().getEnableMetrics(), is(nullValue()));

        MetricsConfig metricsConfig = bridge.getSpec().getMetricsConfig();
        assertThat(metricsConfig, is(notNullValue()));
        assertThat(metricsConfig, is(instanceOf(JmxPrometheusExporterMetrics.class)));

        JmxPrometheusExporterMetrics jmx = (JmxPrometheusExporterMetrics) metricsConfig;
        assertThat(jmx.getValueFrom().getConfigMapKeyRef().getName(), is("test-bridge-jmx-exporter-configuration"));
        assertThat(jmx.getValueFrom().getConfigMapKeyRef().getKey(), is("test-bridge-jmx-exporter-configuration.yaml"));
    }
}