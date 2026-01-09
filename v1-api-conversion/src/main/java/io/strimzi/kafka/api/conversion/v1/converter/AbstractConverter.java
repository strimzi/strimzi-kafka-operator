/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.Conversion;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.ListConversion;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.VersionConversion;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;

/**
 * Abstract converter class for converting between versions of a custom resource
 *
 * @param <T>   Type of the custom resource to convert
 */
public abstract class AbstractConverter<T extends HasMetadata> {
    private final List<VersionConversion<T>> conversions;

    /**
     * Constructor
     *
     * @param conversions   List of conversions available to this convertor
     */
    public AbstractConverter(List<VersionConversion<T>> conversions) {
        // Check the given conversions are ordered
        ApiVersion lastTo = null;
        for (VersionConversion<T> conv : conversions) {
            if (lastTo != null
                    && !lastTo.equals(conv.from())) {
                throw new IllegalArgumentException();
            }
            lastTo = conv.to();
        }
        this.conversions = conversions;
    }

    /**
     * Gets the class of the custom resource to convert.
     *
     * @return  Returns the class of the custom resource to convert
     */
    public abstract Class<T> crClass();

    /**
     * Convert a typed custom resource object to the given version
     *
     * @param gkr           Instance of the custom resource object
     * @param toVersion     Version to convert it to
     *
     * @return Converted generic Kubernetes resource
     */
    public GenericKubernetesResource convertTo(GenericKubernetesResource gkr, ApiVersion toVersion) {
        JsonMapper mapper = new JsonMapper();
        JsonNode genericResourceJson = mapper.valueToTree(gkr);
        convertTo(genericResourceJson, toVersion);

        // Converted, now we update the original object
        return mapper.convertValue(genericResourceJson, GenericKubernetesResource.class);
    }

    /**
     * Convert a typed custom resource object to the given version
     *
     * @param instance      Instance of the custom resource object
     * @param toVersion     Version to convert it to
     */
    public void convertTo(T instance, ApiVersion toVersion) {
        Consumer<ListConversion<T, VersionConversion<T>>> consumer = conversion -> conversion.convert(instance);
        convertTo(consumer, toVersion, apiVersion(instance));
    }

    /**
     * Convert a JSON custom resource object to the given version
     *
     * @param node          Instance of the custom resource JSON object
     * @param toVersion     Version to convert it to
     */
    public void convertTo(JsonNode node, ApiVersion toVersion) {
        Consumer<ListConversion<T, VersionConversion<T>>> consumer = conversion -> conversion.convert(node);
        ApiVersion instanceVersion = parseApiVersion(getApiVersion(node));
        convertTo(consumer, toVersion, instanceVersion);
    }

    private void convertTo(Consumer<ListConversion<T, VersionConversion<T>>> consumer, ApiVersion toVersion, ApiVersion instanceVersion) {
        ListConversion<T, VersionConversion<T>> conversion;
        int cmp = instanceVersion.compareTo(toVersion);

        if (cmp > 0) {
            throw new RuntimeException("Reverse conversions from " + instanceVersion + " to " + toVersion + " are not supported");
        } else if (cmp < 0) {
            int startIndex = -1;
            int endIndex = -1;

            for (int i = 0; i < conversions.size(); i++) {
                VersionConversion<T> c = conversions.get(i);
                if (instanceVersion.equals(c.from())) {
                    startIndex = i;
                }
                if (toVersion.equals(c.to())) {
                    endIndex = i;
                }
            }

            if (startIndex == -1) {
                throw new RuntimeException("Couldn't find conversion from " + instanceVersion);
            }

            if (endIndex == -1) {
                throw new RuntimeException("Couldn't find conversion to " + toVersion);
            }

            conversion = new ListConversion<>(conversions.subList(startIndex, endIndex + 1));
        } else {
            List<VersionConversion<T>> versionConversions = null;

            for (VersionConversion<T> c : conversions) {
                if (instanceVersion.equals(c.from()) && toVersion.equals(c.to())) {
                    versionConversions = List.of(c);
                }
            }

            if (versionConversions == null) {
                throw new RuntimeException("Couldn't find conversion from " + instanceVersion + " to " + toVersion);
            }

            conversion = new ListConversion<>(versionConversions);
        }

        consumer.accept(conversion);
    }

    /**
     * Creates a conversion from one version to another with various API changes
     *
     * @param from          Version to convert from
     * @param to            Version to convert to
     * @param conversions   Additional conversions
     * @return              Conversion between two versions
     * @param <U>           The type to be converted by this conversion
     */
    @SafeVarargs
    protected static <U extends HasMetadata> VersionConversion<U> toVersionConversion(ApiVersion from, ApiVersion to, Conversion<U>... conversions) {
        return new VersionConversion<>(from, to, asList(conversions));
    }

    private static <T extends HasMetadata> ApiVersion apiVersion(T instance) {
        String apiVersion = instance.getApiVersion();
        return parseApiVersion(apiVersion);
    }

    private static ApiVersion parseApiVersion(String apiVersion) {
        if (apiVersion == null || !apiVersion.matches("[a-z.]+/v[0-9]+((alpha|beta)[0-9]+)?")) {
            throw new RuntimeException("Bad api version " + apiVersion);
        }
        return ApiVersion.parse(apiVersion.substring(apiVersion.indexOf("/") + 1));
    }

    /**
     * Get the apiVersion from the given JSON node
     *
     * @param node  Node to get the version from
     *
     * @return  APi version as a string
     */
    public static String getApiVersion(JsonNode node) {
        JsonNode apiVersion = node.get("apiVersion");

        if (apiVersion == null) {
            throw new IllegalArgumentException("Missing apiVersion property!");
        }

        return apiVersion.asText();
    }
}
