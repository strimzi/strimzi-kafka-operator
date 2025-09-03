/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.HasMetadata;

import java.util.List;

/**
 * A conversion from one version of an object to another.
 * @param <T>   The converted type
 * @param <C>   The conversion type
 */
public class ListConversion<T extends HasMetadata, C extends Conversion<T>> implements Conversion<T> {
    private final List<C> conversions;

    /**
     * Constructor
     *
     * @param conversions   List of conversions available to this convertor
     */
    public ListConversion(List<C> conversions) {
        this.conversions = conversions;
    }

    @Override
    public void convert(JsonNode node) {
        for (C conv : conversions) {
            conv.convert(node);
        }
    }

    @Override
    public void convert(T instance) {
        for (C conv : conversions) {
            conv.convert(instance);
        }
    }
}
