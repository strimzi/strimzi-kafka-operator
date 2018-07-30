/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.crdgenerator.annotations.Description;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.sundr.builder.annotations.Buildable;

/**
 * Configures the broker authorization
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaAuthorizationSimple extends KafkaAuthorization {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_SIMPLE = "simple";

    @Description("Must be `" + TYPE_SIMPLE + "`")
    @Override
    public String getType() {
        return TYPE_SIMPLE;
    }
}
