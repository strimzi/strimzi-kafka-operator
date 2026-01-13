/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.kafka.api.conversion.v1.converter.AbstractConverter;

import java.util.List;
import java.util.Objects;

/**
 * Conversion which replaces the API version of a resource.
 *
 * @param <T>   Type of the resource where the conversion should be applied
 */
public class VersionConversion<T extends HasMetadata> extends ListConversion<T, Conversion<T>> {
    private final ReplaceApiVersion<HasMetadata> replaceApiVersion;

    /**
     * Constructor
     *
     * @param from          From API version
     * @param to            To API version
     * @param conversions   The list of conversions to apply when moving to the new API version
     */
    public VersionConversion(ApiVersion from, ApiVersion to, List<Conversion<T>> conversions) {
        super(conversions);
        Objects.requireNonNull(from);
        Objects.requireNonNull(to);
        Objects.requireNonNull(conversions);
        this.replaceApiVersion = new ReplaceApiVersion<>(from, to);
    }

    /**
     * Gets the source API version.
     *
     * @return  From API version
     */
    public ApiVersion from() {
        return replaceApiVersion.from();
    }

    /**
     * Gets the target API version.
     *
     * @return  To API version
     */
    public ApiVersion to() {
        return replaceApiVersion.to();
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
    public String toString() {
        return "VersionConversion(" +
                "from=" + from() +
                ", to=" + to() +
                ')';
    }

    @SuppressWarnings("ClassCanBeRecord")
    static class ReplaceApiVersion<T extends HasMetadata> implements Conversion<T> {
        private final ApiVersion from;
        private final ApiVersion to;

        /**
         * Constructor
         *
         * @param from   From API version
         * @param to     To API version
         */
        public ReplaceApiVersion(ApiVersion from, ApiVersion to) {
            this.from = from;
            this.to = to;
        }

        /**
         * Gets the source API version.
         *
         * @return  From API version
         */
        public ApiVersion from() {
            return from;
        }

        /**
         * Gets the target API version.
         *
         * @return  To API version
         */
        public ApiVersion to() {
            return to;
        }

        @Override
        public void convert(JsonNode node) {
            String oldVersion = AbstractConverter.getApiVersion(node);
            String newVersion = replaceVersion(oldVersion, to);
            ConversionUtil.replace(node, "/apiVersion", (n, c) -> c.textNode(newVersion));
        }

        @Override
        public void convert(T from) {
            from.setApiVersion(replaceVersion(from.getApiVersion(), to));
        }

        /**
         * Replaces the API version in the given "group/version" with the given version
         *
         * @param apiVersion The group/version
         * @param newVersion The new version
         *
         * @return a new "group/version" with the given version
         */
        private static String replaceVersion(String apiVersion, ApiVersion newVersion) {
            if (apiVersion == null || !apiVersion.matches("[a-z.]+/v[0-9]+((alpha|beta)[0-9]+)?")) {
                throw new RuntimeException("Bad API version " + apiVersion);
            }
            return apiVersion.substring(0, apiVersion.indexOf("/") + 1) + newVersion;
        }
    }
}
