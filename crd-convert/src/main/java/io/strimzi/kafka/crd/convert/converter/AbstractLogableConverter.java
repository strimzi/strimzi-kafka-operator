/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;

import static java.util.Arrays.asList;

/**
 * Use this converter for CR's that have logging in its spec.
 *
 * @param <T> the actual custom resource type
 */
public abstract class AbstractLogableConverter<T extends HasMetadata> extends Converter<T> {

    @SuppressWarnings("unchecked")
    public AbstractLogableConverter() {
        super(asList(
            toVersionConversion(ApiVersion.V1ALPHA1, ApiVersion.V1BETA1),
            toVersionConversion(
                ApiVersion.V1BETA1,
                ApiVersion.V1BETA2,
                Conversion.replaceLogging("/spec/logging", "log4j.properties")
            )
        ));
    }
}

