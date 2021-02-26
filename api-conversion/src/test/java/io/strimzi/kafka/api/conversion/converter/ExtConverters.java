/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;

import java.io.IOException;
import java.io.UncheckedIOException;

class ExtConverters {

    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    interface ExtConverter<T extends HasMetadata> {
        T testConvertTo(T instance, ApiVersion toVersion);
    }

    static <T extends HasMetadata> ExtConverter<T> crConverter(Converter<T> converter) {
        return (instance, toVersion) -> {
            converter.convertTo(instance, toVersion);
            return instance;
        };
    }

    static <T extends HasMetadata> ExtConverter<T> nodeConverter(Converter<T> converter) {
        return (instance, toVersion) -> {
            try {
                byte[] bytes = JSON_MAPPER.writeValueAsBytes(instance);
                JsonNode node = JSON_MAPPER.readTree(bytes);
                converter.convertTo(node, toVersion);
                return JSON_MAPPER.readerFor(converter.crClass()).readValue(node);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}