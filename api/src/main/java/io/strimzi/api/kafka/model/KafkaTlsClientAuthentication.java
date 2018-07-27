/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.crdgenerator.annotations.Description;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.sundr.builder.annotations.Buildable;

/**
 * Configures the broker authentication
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaTlsClientAuthentication extends KafkaListenerAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_TLS = "tls";

    @Description("Must be `" + TYPE_TLS + "`")
    @Override
    public String getType() {
        return TYPE_TLS;
    }
}
