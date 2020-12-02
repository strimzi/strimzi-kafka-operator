/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

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

class MetricsConversionTest extends ConversionTestBase<HasConfigurableMetrics> {

    @Test
    public void testMetricsConversion() throws Exception {
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
        conversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getKafka());
        conversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getZookeeper());
        conversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getCruiseControl());

        KafkaConnect kc = new KafkaConnectBuilder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withMetadata(new ObjectMetaBuilder().withName("test").build())
            .withNewSpec()
                .withMetrics(Collections.singletonMap("foo", "bar"))
            .endSpec()
            .build();
        conversion(new KafkaConnectConverter(), kc, ApiVersion.V1BETA2, KafkaConnect::getSpec);

        KafkaConnectS2I kcs2i = new KafkaConnectS2IBuilder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withMetadata(new ObjectMetaBuilder().withName("test").build())
            .withNewSpec()
                .withMetrics(Collections.singletonMap("foo", "bar"))
            .endSpec()
            .build();
        conversion(new KafkaConnectS2IConverter(), kcs2i, ApiVersion.V1BETA2, KafkaConnectS2I::getSpec);

        KafkaMirrorMaker kmm = new KafkaMirrorMakerBuilder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withMetadata(new ObjectMetaBuilder().withName("test").build())
            .withNewSpec()
                .withMetrics(Collections.singletonMap("foo", "bar"))
            .endSpec()
            .build();
        conversion(new KafkaMirrorMakerConverter(), kmm, ApiVersion.V1BETA2, KafkaMirrorMaker::getSpec);

        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withMetadata(new ObjectMetaBuilder().withName("test").build())
            .withNewSpec()
                .withMetrics(Collections.singletonMap("foo", "bar"))
            .endSpec()
            .build();
        conversion(new KafkaMirrorMaker2Converter(), kmm2, ApiVersion.V1BETA2, KafkaMirrorMaker2::getSpec);
    }

    void assertions(HasConfigurableMetrics metrics) {
        Assertions.assertNull(metrics.getMetrics());
        MetricsConfig metricsConfig = metrics.getMetricsConfig();
        Assertions.assertNotNull(metricsConfig);
        Assertions.assertTrue(metricsConfig instanceof JmxPrometheusExporterMetrics);
        JmxPrometheusExporterMetrics jmx = (JmxPrometheusExporterMetrics) metricsConfig;
        ExternalConfigurationReference valueFrom = jmx.getValueFrom();
        Assertions.assertNotNull(valueFrom);
        ConfigMapKeySelector ref = valueFrom.getConfigMapKeyRef();
        Assertions.assertNotNull(ref);
        Assertions.assertEquals("test-" + metrics.getTypeName() + "-jmx-exporter-configuration-name", ref.getName());
    }
}