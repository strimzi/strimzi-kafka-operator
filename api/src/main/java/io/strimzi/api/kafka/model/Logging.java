/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.crdgenerator.annotations.Description;

import java.io.Serializable;

/**
 * Describes the logging configuration
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = InlineLogging.TYPE_INLINE, value = InlineLogging.class),
        @JsonSubTypes.Type(name = ExternalLogging.TYPE_EXTERNAL, value = ExternalLogging.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class Logging implements Serializable {

    private static final long serialVersionUID = 1L;

    @Description("Logging type, must be either 'inline' or 'external'.")
    public abstract String getType();
}

