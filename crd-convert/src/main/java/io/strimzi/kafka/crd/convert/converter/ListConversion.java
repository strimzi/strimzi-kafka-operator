/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * A conversion from one version of an object to another.
 * @param <T> The converted type
 */
class ListConversion<T extends HasMetadata, C extends Conversion<T>> implements Conversion<T> {
    private final List<C> conversions;

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

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ListConversion<T, C> reverse() {
        List inverted = reversed();
        return new ListConversion<T, C>(inverted);
    }

    public List<Conversion<T>> reversed() {
        List<Conversion<T>> inverted = conversions.stream()
                .map(Conversion::reverse)
                .collect(Collectors.toList());
        Collections.reverse(inverted);
        return inverted;
    }

}
