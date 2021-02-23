/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.ExternalConfigurationReference;
import io.strimzi.api.kafka.model.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.MetricsConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class MetricsConversionTest extends AbstractConversionTestBase<HasConfigurableMetrics> {
    @Test
    public void testKafkaMetricsConversion() throws Exception {
        Kafka k = new KafkaBuilder()
                .withApiVersion("kafka.strimzi.io/v1beta1")
                .withMetadata(new ObjectMetaBuilder().withName("test").build())
                .withNewSpec()
                .withNewKafka()
                .withMetrics(Collections.singletonMap("foo", "bar"))
                .endKafka()
                .withNewZookeeper()
                .withMetrics(Collections.singletonMap("foo", "bar"))
                .endZookeeper()
                .withNewCruiseControl()
                .withMetrics(Collections.singletonMap("foo", "bar"))
                .endCruiseControl()
                .endSpec()
                .build();

        KafkaConverter converter = new KafkaConverter();

        assertions(crConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getKafka()), "kafka");
        assertions(crConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getZookeeper()), "zookeeper");
        assertions(crConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getCruiseControl()), "cruise-control");

        assertions(jsonConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getKafka()), "kafka");
        assertions(jsonConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getZookeeper()), "zookeeper");
        assertions(jsonConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getCruiseControl()), "cruise-control");
    }

    @Test
    public void testConnectMetricsConversion() throws Exception {
        KafkaConnect kc = new KafkaConnectBuilder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withMetadata(new ObjectMetaBuilder().withName("test").build())
            .withNewSpec()
                .withMetrics(Collections.singletonMap("foo", "bar"))
            .endSpec()
            .build();

        assertions(crConversion(new KafkaConnectConverter(), kc, ApiVersion.V1BETA2, KafkaConnect::getSpec), "connect");
        assertions(jsonConversion(new KafkaConnectConverter(), kc, ApiVersion.V1BETA2, KafkaConnect::getSpec), "connect");
    }

    @Test
    public void testConnectS2IMetricsConversion() throws Exception {
        KafkaConnectS2I kcs2i = new KafkaConnectS2IBuilder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withMetadata(new ObjectMetaBuilder().withName("test").build())
            .withNewSpec()
                .withMetrics(Collections.singletonMap("foo", "bar"))
            .endSpec()
            .build();

        assertions(crConversion(new KafkaConnectS2IConverter(), kcs2i, ApiVersion.V1BETA2, KafkaConnectS2I::getSpec), "connect-s2i");
        assertions(jsonConversion(new KafkaConnectS2IConverter(), kcs2i, ApiVersion.V1BETA2, KafkaConnectS2I::getSpec), "connect-s2i");
    }

    @Test
    public void testMirrorMakerMetricsConversion() throws Exception {
        KafkaMirrorMaker kmm = new KafkaMirrorMakerBuilder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withMetadata(new ObjectMetaBuilder().withName("test").build())
            .withNewSpec()
                .withMetrics(Collections.singletonMap("foo", "bar"))
            .endSpec()
            .build();

        assertions(crConversion(new KafkaMirrorMakerConverter(), kmm, ApiVersion.V1BETA2, KafkaMirrorMaker::getSpec), "mirror-maker");
        assertions(jsonConversion(new KafkaMirrorMakerConverter(), kmm, ApiVersion.V1BETA2, KafkaMirrorMaker::getSpec), "mirror-maker");
    }

    @Test
    public void testMirrorMaker2MetricsConversion() throws Exception {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withMetadata(new ObjectMetaBuilder().withName("test").build())
            .withNewSpec()
                .withMetrics(Collections.singletonMap("foo", "bar"))
            .endSpec()
            .build();

        assertions(crConversion(new KafkaMirrorMaker2Converter(), kmm2, ApiVersion.V1BETA2, KafkaMirrorMaker2::getSpec), "mirror-maker-2");
        assertions(jsonConversion(new KafkaMirrorMaker2Converter(), kmm2, ApiVersion.V1BETA2, KafkaMirrorMaker2::getSpec), "mirror-maker-2");
    }

    void assertions(HasConfigurableMetrics metrics, String type) {
        Assertions.assertNull(metrics.getMetrics());
        MetricsConfig metricsConfig = metrics.getMetricsConfig();
        Assertions.assertNotNull(metricsConfig);
        Assertions.assertTrue(metricsConfig instanceof JmxPrometheusExporterMetrics);
        JmxPrometheusExporterMetrics jmx = (JmxPrometheusExporterMetrics) metricsConfig;
        ExternalConfigurationReference valueFrom = jmx.getValueFrom();
        Assertions.assertNotNull(valueFrom);
        ConfigMapKeySelector ref = valueFrom.getConfigMapKeyRef();
        Assertions.assertNotNull(ref);
        Assertions.assertEquals("test-" + type + "-jmx-exporter-configuration", ref.getName());
        Assertions.assertEquals("test-" + type + "-jmx-exporter-configuration.yaml", ref.getKey());
    }
}