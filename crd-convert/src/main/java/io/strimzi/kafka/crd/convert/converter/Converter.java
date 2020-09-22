/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;

public abstract class Converter<T extends HasMetadata> {
    private final List<VersionConversion<T>> conversions;

    public Converter(List<VersionConversion<T>> conversions) {
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

    public abstract Class<T> crClass();

    private static <T extends HasMetadata> ApiVersion apiVersion(T instance) {
        String apiVersion = instance.getApiVersion();
        if (apiVersion == null || !apiVersion.matches("[a-z.]+/v[0-9]+((alpha|beta)[0-9]+)?")) {
            throw new RuntimeException("Bad api version " + apiVersion);
        }
        return ApiVersion.parse(apiVersion.substring(apiVersion.indexOf("/") + 1));
    }

    public void convertTo(T instance, ApiVersion toVersion) {
        ApiVersion instanceVersion = apiVersion(instance);
        int cmp = instanceVersion.compareTo(toVersion);
        if (cmp == 0) {
            return;
        } else {
            int startIndex = -1;
            int endIndex = -1;
            boolean forward = cmp == -1;
            for (int i = 0; i < conversions.size(); i++) {
                VersionConversion<T> conversion = conversions.get(i);
                if (instanceVersion.equals(forward ? conversion.from() : conversion.to())) {
                    startIndex = i;
                }
                if (toVersion.equals(forward ? conversion.to() : conversion.from())) {
                    endIndex = i;
                }
            }
            if (startIndex == -1) {
                throw new RuntimeException("Couldn't find conversion from " + instanceVersion);
            }
            if (endIndex == -1) {
                throw new RuntimeException("Couldn't find conversion to " + toVersion);
            }
            List<VersionConversion<T>> versionConversions = conversions.subList(startIndex, endIndex + 1);
            ListConversion<T, VersionConversion<T>> conversion;
            if (forward) {
                conversion = new ListConversion<>(versionConversions);
            } else {
                versionConversions = versionConversions.stream().map(c -> c.reverse()).collect(Collectors.toList());
                Collections.reverse(versionConversions);
                conversion = new ListConversion<>(versionConversions);
            }
            conversion.convert(instance);
        }
    }
}
