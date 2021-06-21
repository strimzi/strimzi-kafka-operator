/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class VersionConversion<T extends HasMetadata> extends ListConversion<T, Conversion<T>> {

    private final ReplaceApiVersion<HasMetadata> replaceApiVersion;

    VersionConversion(ApiVersion from, ApiVersion to) {
        this(from, to, Collections.emptyList());
    }

    VersionConversion(ApiVersion from, ApiVersion to, List<Conversion<T>> conversions) {
        super(conversions);
        Objects.requireNonNull(from);
        Objects.requireNonNull(to);
        Objects.requireNonNull(conversions);
        this.replaceApiVersion = Conversion.replaceApiVersion(from, to);
    }

    public ApiVersion from() {
        return replaceApiVersion.getFromVersion();
    }

    public ApiVersion to() {
        return replaceApiVersion.getToVersion();
    }

    @Override
    public void convert(JsonNode node) {
        super.convert(node);
        replaceApiVersion.convert(node);
    }

    /**
     * Convert the given object {@linkplain #from() from one version} to {@linkplain #to() another}.
     * @param instance The object to convert
     */
    @Override
    public void convert(T instance) {
        super.convert(instance);
        replaceApiVersion.convert(instance);
    }

    @Override
    public VersionConversion<T> reverse() {
        return new VersionConversion<T>(replaceApiVersion.getToVersion(), replaceApiVersion.getFromVersion(), super.reversed());
    }

    @Override
    public String toString() {
        return "VersionConversion(" +
                "from=" + from() +
                ", to=" + to() +
                ')';
    }
}
