/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class VersionConversionTest {
    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    @Test
    public void testVersionConversion() throws Exception {
        KafkaConnector kc = new KafkaConnectorBuilder().withApiVersion("kafka.strimzi.io/v1beta1").build();
        versionConversion(new KafkaConnectorConverter(), kc, ApiVersion.V1BETA2);

        KafkaRebalance kr = new KafkaRebalanceBuilder().withApiVersion("kafka.strimzi.io/v1beta1").build();
        versionConversion(new KafkaRebalanceConverter(), kr, ApiVersion.V1BETA2);

        KafkaTopic kt = new KafkaTopicBuilder().withApiVersion("kafka.strimzi.io/v1beta1").build();
        versionConversion(new KafkaTopicConverter(), kt, ApiVersion.V1BETA2);

        KafkaUser ku = new KafkaUserBuilder().withApiVersion("kafka.strimzi.io/v1beta1").build();
        versionConversion(new KafkaUserConverter(), ku, ApiVersion.V1BETA2);
    }

    private <T extends HasMetadata> void versionConversion(
        Converter<T> converter,
        T cr,
        ApiVersion toApiVersion
    ) throws Exception {
        // first do json node conversion, so we don't modify cr already
        byte[] bytes = JSON_MAPPER.writeValueAsBytes(cr);
        JsonNode node = JSON_MAPPER.readTree(bytes);
        converter.convertTo(node, toApiVersion);
        T converted = JSON_MAPPER.readerFor(converter.crClass()).readValue(node);
        String apiVersion = converted.getApiVersion();
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", apiVersion);

        converter.convertTo(cr, toApiVersion);
        apiVersion = converted.getApiVersion();
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", apiVersion);
    }
}