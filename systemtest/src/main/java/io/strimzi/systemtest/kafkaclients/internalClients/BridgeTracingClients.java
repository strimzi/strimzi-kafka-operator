/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.strimzi.systemtest.tracing.TracingConstants;
import io.sundr.builder.annotations.Buildable;

@Buildable(editableEnabled = false)
public class BridgeTracingClients extends BridgeClients {

    private String tracingServiceNameEnvVar;
    private boolean openTelemetry = false;
    private String tracingType;

    public String getTracingServiceNameEnvVar() {

        return tracingServiceNameEnvVar;
    }

    public void setTracingServiceNameEnvVar(String tracingServiceNameEnvVar) {
        this.tracingServiceNameEnvVar = tracingServiceNameEnvVar;
    }

    private String serviceNameEnvVar() {
        // use tracing service name env var if set, else use "dummy"
        return tracingServiceNameEnvVar != null ? tracingServiceNameEnvVar : "TEST_SERVICE_NAME";
    }

    public boolean isOpenTelemetry() {
        return openTelemetry;
    }

    public void setOpenTelemetry(boolean openTelemetry) {
        this.openTelemetry = openTelemetry;
    }

    public void setTracingType(String tracingType) {
        // if `withOpenTelemetry` or `withOpenTracing` is used, this is the only way how to set it also as the tracingType
        // to remove need of extra check in each client's method
        if (this.openTelemetry) {
            this.tracingType = TracingConstants.OPEN_TELEMETRY;
        } else {
            this.tracingType = tracingType;
        }
    }

    public String getTracingType() {
        return tracingType;
    }

    public Job producerStrimziBridgeWithTracing() {
        return this.defaultProducerStrimziBridge()
                .editSpec()
                    .editTemplate()
                        .editSpec()
                            .editFirstContainer()
                                .addNewEnv()
                                    .withName(this.serviceNameEnvVar())
                                    .withValue(this.getProducerName())
                                .endEnv()
                                // this will only get used if tracing is enabled -- see serviceNameEnvVar()
                                .addNewEnv()
                                    .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                                    .withValue(TracingConstants.JAEGER_COLLECTOR_OTLP_URL)
                                .endEnv()
                                .addNewEnv()
                                    .withName("TRACING_TYPE")
                                    .withValue(this.tracingType)
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    public Job consumerStrimziBridgeWithTracing() {
        return this.defaultConsumerStrimziBridge()
                .editSpec()
                    .editTemplate()
                        .editSpec()
                            .editFirstContainer()
                                .addNewEnv()
                                    .withName(this.serviceNameEnvVar())
                                    .withValue(this.getConsumerName())
                                .endEnv()
                                // this will only get used if tracing is enabled -- see serviceNameEnvVar()
                                .addNewEnv()
                                    .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                                    .withValue(TracingConstants.JAEGER_COLLECTOR_OTLP_URL)
                                .endEnv()
                                .addNewEnv()
                                    .withName("TRACING_TYPE")
                                    .withValue(this.tracingType)
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }
}
