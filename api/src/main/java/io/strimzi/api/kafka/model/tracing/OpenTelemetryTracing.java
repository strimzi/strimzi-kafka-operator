/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.tracing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Configures the tracing using the Jaeger OpenTelemetry implementation
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type"})
@EqualsAndHashCode
public class OpenTelemetryTracing extends Tracing {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_OPENTELEMETRY = "opentelemetry";

    public static final String CONSUMER_INTERCEPTOR_CLASS_NAME = "io.opentelemetry.instrumentation.kafkaclients.TracingConsumerInterceptor";
    public static final String PRODUCER_INTERCEPTOR_CLASS_NAME = "io.opentelemetry.instrumentation.kafkaclients.TracingProducerInterceptor";

    @Description("Must be `" + TYPE_OPENTELEMETRY + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_OPENTELEMETRY;
    }
}
