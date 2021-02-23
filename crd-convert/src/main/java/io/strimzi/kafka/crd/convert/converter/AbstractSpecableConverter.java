/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.HasConfigurableMetrics;

import static java.util.Arrays.asList;

/**
 * Use this converter for CR's that have tolerations, affinity and logging in its spec.
 *
 * @param <T> the actual custom resource type
 */
public abstract class AbstractSpecableConverter<T extends HasMetadata> extends Converter<T> {
    @SuppressWarnings("unchecked")
    public AbstractSpecableConverter(Class<? extends HasConfigurableMetrics> crClass, String typeName) {
        super(asList(
            toVersionConversion(ApiVersion.V1ALPHA1, ApiVersion.V1BETA1),
            toVersionConversion(
                ApiVersion.V1BETA1,
                ApiVersion.V1BETA2,
                Conversion.move("/spec/tolerations", "/spec/template/pod/tolerations", Conversion.noop()),
                Conversion.move("/spec/affinity", "/spec/template/pod/affinity", Conversion.noop()),
                Conversion.replaceLogging("/spec/logging", "log4j.properties"),
                new MetricsConversion<T>("/spec", crClass, typeName)
            )
        ));
    }
}

