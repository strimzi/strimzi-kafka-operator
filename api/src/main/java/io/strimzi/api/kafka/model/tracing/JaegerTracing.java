/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.tracing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Configures the tracing using the Jaeger OpenTracing implementation
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type"})
@EqualsAndHashCode
@Deprecated
@DeprecatedType(replacedWithType = void.class)
public class JaegerTracing extends Tracing {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_JAEGER = "jaeger";

    public static final String CONSUMER_INTERCEPTOR_CLASS_NAME = "io.opentracing.contrib.kafka.TracingConsumerInterceptor";
    public static final String PRODUCER_INTERCEPTOR_CLASS_NAME = "io.opentracing.contrib.kafka.TracingProducerInterceptor";

    @Description("Must be `" + TYPE_JAEGER + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_JAEGER;
    }
}
