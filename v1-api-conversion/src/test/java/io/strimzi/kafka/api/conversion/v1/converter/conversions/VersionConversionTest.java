/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.kafka.api.conversion.v1.converter.AbstractConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaConnectorConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaRebalanceConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class VersionConversionTest {
    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    @Test
    public void testVersionConversion() throws Exception {
        KafkaConnector kc = new KafkaConnectorBuilder().withApiVersion("kafka.strimzi.io/v1beta2").withNewSpec().endSpec().build();
        versionConversion(new KafkaConnectorConverter(), kc, ApiVersion.V1);

        KafkaRebalance kr = new KafkaRebalanceBuilder().withApiVersion("kafka.strimzi.io/v1beta2").withNewSpec().endSpec().build();
        versionConversion(new KafkaRebalanceConverter(), kr, ApiVersion.V1);
    }

    private <T extends HasMetadata> void versionConversion(
        AbstractConverter<T> converter,
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