/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.ExternalConfigurationReference;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.Logging;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LoggingConversionTest extends AbstractConversionTestBase<Logging> {

    @Test
    public void testKafkaLoggingConversion() throws Exception {
        Kafka k = new KafkaBuilder()
                .withApiVersion("kafka.strimzi.io/v1beta1")
                .withNewSpec()
                    .withNewKafka()
                        .withLogging(new ExternalLoggingBuilder().withName("foo").build())
                    .endKafka()
                    .withNewZookeeper()
                        .withLogging(new ExternalLoggingBuilder().withName("foo").build())
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                            .withLogging(new ExternalLoggingBuilder().withName("foo").build())
                        .endTopicOperator()
                        .withNewUserOperator()
                            .withLogging(new ExternalLoggingBuilder().withName("foo").build())
                        .endUserOperator()
                    .endEntityOperator()
                    .withNewCruiseControl()
                        .withLogging(new ExternalLoggingBuilder().withName("foo").build())
                    .endCruiseControl()
                .endSpec()
                .build();

        KafkaConverter converter = new KafkaConverter();

        assertions(crConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getKafka().getLogging()));
        assertions(crConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getZookeeper().getLogging()));
        assertions(crConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getEntityOperator().getTopicOperator().getLogging()));
        assertions(crConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getEntityOperator().getUserOperator().getLogging()));
        assertions(crConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getCruiseControl().getLogging()));

        assertions(jsonConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getKafka().getLogging()));
        assertions(jsonConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getZookeeper().getLogging()));
        assertions(jsonConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getEntityOperator().getTopicOperator().getLogging()));
        assertions(jsonConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getEntityOperator().getUserOperator().getLogging()));
        assertions(jsonConversion(converter, k, ApiVersion.V1BETA2, v -> v.getSpec().getCruiseControl().getLogging()));
    }

    @Test
    public void testKafkaConnectLoggingConversion() throws Exception {
        KafkaConnect kc = new KafkaConnectBuilder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withNewSpec()
                .withLogging(new ExternalLoggingBuilder().withName("foo").build())
            .endSpec()
            .build();

        assertions(crConversion(new KafkaConnectConverter(), kc, ApiVersion.V1BETA2, v -> v.getSpec().getLogging()));
        assertions(jsonConversion(new KafkaConnectConverter(), kc, ApiVersion.V1BETA2, v -> v.getSpec().getLogging()));
    }

    @Test
    public void testKafkaConnectS2ILoggingConversion() throws Exception {
        KafkaConnectS2I kcs2i = new KafkaConnectS2IBuilder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withNewSpec()
                .withLogging(new ExternalLoggingBuilder().withName("foo").build())
            .endSpec()
            .build();

        assertions(crConversion(new KafkaConnectS2IConverter(), kcs2i, ApiVersion.V1BETA2, v -> v.getSpec().getLogging()));
        assertions(jsonConversion(new KafkaConnectS2IConverter(), kcs2i, ApiVersion.V1BETA2, v -> v.getSpec().getLogging()));
    }

    @Test
    public void testKafkaMirrorMakerLoggingConversion() throws Exception {
        KafkaMirrorMaker kmm = new KafkaMirrorMakerBuilder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withNewSpec()
                .withLogging(new ExternalLoggingBuilder().withName("foo").build())
            .endSpec()
            .build();

        assertions(crConversion(new KafkaMirrorMakerConverter(), kmm, ApiVersion.V1BETA2, v -> v.getSpec().getLogging()));
        assertions(jsonConversion(new KafkaMirrorMakerConverter(), kmm, ApiVersion.V1BETA2, v -> v.getSpec().getLogging()));
    }

    @Test
    public void testKafkaMirrorMAker2LoggingConversion() throws Exception {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withNewSpec()
                .withLogging(new ExternalLoggingBuilder().withName("foo").build())
            .endSpec()
            .build();

        assertions(crConversion(new KafkaMirrorMaker2Converter(), kmm2, ApiVersion.V1BETA2, v -> v.getSpec().getLogging()));
        assertions(jsonConversion(new KafkaMirrorMaker2Converter(), kmm2, ApiVersion.V1BETA2, v -> v.getSpec().getLogging()));
    }

    @Test
    public void testKafkaBridgeLoggingConversion() throws Exception {
        KafkaBridge kb = new KafkaBridgeBuilder()
            .withApiVersion("kafka.strimzi.io/v1beta1")
            .withNewSpec()
                .withLogging(new ExternalLoggingBuilder().withName("foo").build())
            .endSpec()
            .build();

        assertions(crConversion(new KafkaBridgeConverter(), kb, ApiVersion.V1BETA2, v -> v.getSpec().getLogging()));
        assertions(jsonConversion(new KafkaBridgeConverter(), kb, ApiVersion.V1BETA2, v -> v.getSpec().getLogging()));
    }

    void assertions(Logging logging) {
        if (logging instanceof ExternalLogging) {
            ExternalLogging el = (ExternalLogging) logging;
            Assertions.assertNull(el.getName());
            ExternalConfigurationReference valueFrom = el.getValueFrom();
            Assertions.assertNotNull(valueFrom);
            ConfigMapKeySelector configMapKeyRef = valueFrom.getConfigMapKeyRef();
            Assertions.assertNotNull(configMapKeyRef);
            Assertions.assertNotNull(configMapKeyRef.getKey());
            Assertions.assertNotNull(configMapKeyRef.getName());
            Assertions.assertEquals("foo", configMapKeyRef.getName());
        }
    }
}