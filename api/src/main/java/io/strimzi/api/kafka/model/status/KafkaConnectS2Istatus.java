/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Represents a status of the Kafka Connect S2I resource
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "httpRestApiAddress" })
@EqualsAndHashCode
public class KafkaConnectS2Istatus extends Status {
    private static final long serialVersionUID = 1L;

    private String httpRestApiAddress;

    @Description("HTTP REST API address")
    public String getHttpRestApiAddress() {
        return httpRestApiAddress;
    }

    public void setHttpRestApiAddress(String httpRestApiAddress) {
        this.httpRestApiAddress = httpRestApiAddress;
    }
}