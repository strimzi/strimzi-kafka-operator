/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation for options to be passed to a JVM.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@EqualsAndHashCode
public class EntityOperatorJvmOptions implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private boolean gcLoggingEnabled = true;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Specifies whether the Garbage Collection logging is enabled. The default is true.")
    public boolean isGcLoggingEnabled() {
        return gcLoggingEnabled;
    }

    public void setGcLoggingEnabled(boolean gcLoggingEnabled) {
        this.gcLoggingEnabled = gcLoggingEnabled;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}

