/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;

import java.util.function.Function;

abstract class AbstractConversionTestBase<V> {
    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    protected <T extends HasMetadata> V jsonConversion(Converter<T> converter, T cr, ApiVersion toApiVersion, Function<T, V> fn) throws Exception {
        // first do json node conversion, so we don't modify cr already
        byte[] bytes = JSON_MAPPER.writeValueAsBytes(cr);
        JsonNode node = JSON_MAPPER.readTree(bytes);
        converter.convertTo(node, toApiVersion);
        T converted = JSON_MAPPER.readerFor(converter.crClass()).readValue(node);
        V value = fn.apply(converted);
        return value;
    }

    protected <T extends HasMetadata> V crConversion(Converter<T> converter, T cr, ApiVersion toApiVersion, Function<T, V> fn) {
        converter.convertTo(cr, toApiVersion);
        V value = fn.apply(cr);

        return value;
    }
}